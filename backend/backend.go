package backend

import (
	"context"
	"io"

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
		db: db,
	}, nil
}

type backend struct {
	db store.Store
}

func (b *backend) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	topicStore, err := b.db.TopicStore(req.Topic)
	if err != nil {
		return nil, err
	}
	return &badgerSink{
		store: topicStore,
	}, nil
}

func (b *backend) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	topicStore, err := b.db.TopicStore(req.Topic)
	if err != nil {
		return nil, err
	}
	return &badgerSource{
		store: topicStore,
	}, nil
}

func (b *backend) Close() error {
	return b.db.Close()
}
