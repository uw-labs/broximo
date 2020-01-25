package backend

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/uw-labs/broximo/store"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

type badgerSink struct {
	topic  string
	store  store.TopicStore
	logger *logrus.Logger
}

func (sink *badgerSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	// TODO: if we can configure channel sizes in proximo,
	//  we will be able to get rid of this channel and goroutine
	toAck := make(chan substrate.Message, 100)
	rg, ctx := rungroup.New(ctx)

	rg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
			case msg := <-toAck:
				select {
				case <-ctx.Done():
				case acks <- msg:
				}
			}
		}
	})
	rg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-messages:
				if err := sink.store.SaveMessage(msg.Data()); err != nil {
					// TODO: handle tx retries
					return err
				}
				sink.logger.Debugf("Message written to topic %s: %s", sink.topic, string(msg.Data()))

				if dMsg, ok := msg.(substrate.DiscardableMessage); ok {
					dMsg.DiscardPayload()
				}
				select {
				case <-ctx.Done():
					return nil
				case acks <- msg:
				}
			}
		}
	})
	return rg.Wait()
}

func (sink *badgerSink) Close() error {
	return nil
}

func (sink *badgerSink) Status() (*substrate.Status, error) {
	return &substrate.Status{
		Working: true,
	}, nil
}
