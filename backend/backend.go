package backend

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	Logger             *logrus.Logger
	BadgerDBPath       string
	BadgerMaxCacheSize int64
}

// New creates new Backend instance from the provided config.
func New(c Config) (Backend, error) {
	db, err := store.NewBadgerStore(c.BadgerDBPath, c.BadgerMaxCacheSize, c.Logger)
	if err != nil {
		return nil, err
	}
	return &backend{
		db:              db,
		logger:          c.Logger,
		topics:          map[string]store.TopicStore{},
		activeConsumers: map[string]bool{},
	}, nil
}

type backend struct {
	db     store.Store
	logger *logrus.Logger

	topicsMutex sync.Mutex
	topics      map[string]store.TopicStore

	consumersMutex  sync.Mutex
	activeConsumers map[string]bool
}

func (b *backend) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	topicStore, err := b.newTopicStore(req.Topic)
	if err != nil {
		return nil, err
	}
	b.logger.Debugf("New sink for topic '%s' created.", req.Topic)

	return &badgerSink{
		topic:  req.Topic,
		store:  topicStore,
		logger: b.logger,
	}, nil
}

func (b *backend) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	if isNameInvalid(req.Consumer) {
		return nil, errors.Errorf("invalid consumer name %s", req.Consumer)
	}

	topicStore, err := b.newTopicStore(req.Topic)
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
	b.logger.Debugf("New consumer for topic '%s' with id '%s'.", req.Topic, req.Consumer)

	return &badgerSource{
		topic:         req.Topic,
		consumerID:    req.Consumer,
		initialOffset: offset,
		backend:       b,
		store:         topicStore,
		logger:        b.logger,
	}, nil
}

func (b *backend) Close() error {
	return b.db.Close()
}

func (b *backend) newTopicStore(topicName string) (topicStore store.TopicStore, err error) {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	if isNameInvalid(topicName) {
		return nil, errors.Errorf("invalid topic name '%s'", topicName)
	}
	topicStore, ok := b.topics[topicName]
	if !ok {
		topicStore, err = b.db.TopicStore(topicName)
		if err != nil {
			return nil, err
		}
		b.topics[topicName] = topicStore
	}

	return topicStore, nil
}

func (b *backend) markConsumerAsActive(consumerID string) error {
	b.consumersMutex.Lock()
	defer b.consumersMutex.Unlock()

	if b.activeConsumers[consumerID] {
		return errors.Errorf("consumer with id %s is already active", consumerID)
	}
	return nil
}

func (b *backend) markConsumerAsInactive(consumerID string) {
	b.consumersMutex.Lock()
	delete(b.activeConsumers, consumerID)
	b.consumersMutex.Unlock()
}

func isNameInvalid(name string) bool {
	return strings.Contains(name, ":")
}
