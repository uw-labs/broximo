package backend

import (
	"context"

	"github.com/uw-labs/broximo/store"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

type badgerSource struct {
	store store.TopicStore
}

func (source *badgerSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	rg, ctx := rungroup.New(ctx)
	rg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-acks:
				// ignore acks
			}
		}
	})
	rg.Go(func() error {
		return source.store.ReadAllMessages(func(msg []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case messages <- &byteMessage{data: msg}:
				return nil
			}
		})
	})

	return rg.Wait()
}

func (source *badgerSource) Close() error {
	return nil
}

func (source *badgerSource) Status() (*substrate.Status, error) {
	return &substrate.Status{
		Working: true,
	}, nil
}

type byteMessage struct {
	data []byte
}

func (msg *byteMessage) Data() []byte {
	return msg.data
}

func (msg *byteMessage) DiscardPayload() {
	msg.data = nil
}
