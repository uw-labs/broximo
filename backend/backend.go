package backend

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/uw-labs/broximo/store"
	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

// Backend is an extension of proximo.AsyncSinkSourceFactory that implements
// the broximo messaging layer logic.
type Backend interface {
	proximo.AsyncSinkSourceFactory
	io.Closer
}

// Config is a configuration of the backend layer.
type Config struct {
	BadgerDBPath       string
	BadgerMaxCacheSize int64
}

// New creates new Backend instance from the provided config.
func New(c Config) (Backend, error) {
	db, err := store.NewBadgerStore(c.BadgerDBPath, c.BadgerMaxCacheSize)
	if err != nil {
		return nil, err
	}
	return &backend{
		db:              db,
		topics:          map[string]store.TopicStore{},
		activeConsumers: map[string]bool{},
	}, nil
}

type backend struct {
	db              store.Store
	mutex           sync.Mutex
	topics          map[string]store.TopicStore
	activeConsumers map[string]bool
}

func (b *backend) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	topicStore, err := b.topicStoreLocked(req.Topic)
	if err != nil {
		return nil, err
	}
	return &badgerSink{
		store: topicStore,
	}, nil
}

func (b *backend) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if isNameInvalid(req.Consumer) {
		return nil, errors.Errorf("invalid consumer name %s", req.Consumer)
	}
	if b.activeConsumers[req.Consumer] {
		return nil, errors.Errorf("consumer with id %s is already active", req.Consumer)
	}

	topicStore, err := b.topicStoreLocked(req.Topic)
	if err != nil {
		return nil, err
	}
	initialOffset := store.ConsumerOffsetOldest
	if req.InitialOffset == proto.Offset_OFFSET_NEWEST {
		initialOffset = store.ConsumerOffsetNewest
	}
	offset, err := topicStore.GetOrCreateConsumer(req.Consumer, initialOffset)
	if err != nil {
		return nil, err
	}
	b.activeConsumers[req.Consumer] = true

	return &badgerSource{
		consumerID:    req.Consumer,
		initialOffset: offset,
		backend:       b,
		store:         topicStore,
	}, nil
}

func (b *backend) Close() error {
	return b.db.Close()
}

// topicStoreLocked requires calling goroutine to hold the internal lock of this object.
func (b *backend) topicStoreLocked(topicName string) (store.TopicStore, error) {
	if isNameInvalid(topicName) {
		return nil, errors.Errorf("invalid topic name %s", topicName)
	}
	topicStore, ok := b.topics[topicName]
	if !ok {
		topicStore, err := b.db.TopicStore(topicName)
		if err != nil {
			return nil, err
		}
		b.topics[topicName] = topicStore
	}

	return topicStore, nil
}

func (b *backend) closeConsumer(consumerID string) error {
	b.mutex.Lock()
	delete(b.activeConsumers, consumerID)
	b.mutex.Unlock()

	return nil
}

func isNameInvalid(name string) bool {
	return !strings.Contains(name, ":")
}
