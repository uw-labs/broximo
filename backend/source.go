package backend

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/uw-labs/broximo/store"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

type badgerSource struct {
	consumerID    string
	topic         string
	initialOffset uint64
	backend       *backend
	store         store.TopicStore
	logger        *logrus.Logger
}

func (source *badgerSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	if err := source.backend.markConsumerAsActive(source.consumerID); err != nil {
		return err
	}
	rg, ctx := rungroup.New(ctx)
	rg.Go(func() error {
		<-ctx.Done()
		source.backend.markConsumerAsInactive(source.consumerID)
		return nil
	})
	rg.Go(func() error {
		return source.writeMessages(ctx, messages)
	})
	rg.Go(func() error {
		return source.processAcks(ctx, acks)
	})
	return rg.Wait()
}

func (source *badgerSource) writeMessages(ctx context.Context, messages chan<- substrate.Message) error {
	nextSeq := source.initialOffset + 1
	for {
		msg, err := source.store.GetMessage(ctx, nextSeq, true)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case messages <- &seqMessage{data: msg, seq: nextSeq}:
			source.logger.Debugf(
				"Message %v on topic '%s' was sent to consumer '%s'.",
				nextSeq, source.topic, source.consumerID,
			)
			nextSeq++
		}
	}
}

func (source *badgerSource) processAcks(ctx context.Context, acks <-chan substrate.Message) error {
	expectedSeq := source.initialOffset + 1
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-acks:
			sMsg, ok := msg.(*seqMessage)
			if !ok || sMsg.seq != expectedSeq {
				return substrate.InvalidAckError{
					Acked: msg,
					Expected: &seqMessage{
						seq: expectedSeq,
					},
				}
			}
			// TODO: maybe do this less often to make it more efficient.
			if err := source.store.SetConsumerOffset(source.consumerID, expectedSeq); err != nil {
				return err
			}
			source.logger.Debugf(
				"Message %v on topic '%s' was acknowledged by consumer '%s'.",
				expectedSeq, source.topic, source.consumerID,
			)
			expectedSeq++
		}
	}
}

func (source *badgerSource) Close() error {
	return nil
}

func (source *badgerSource) Status() (*substrate.Status, error) {
	return &substrate.Status{
		Working: true,
	}, nil
}

type seqMessage struct {
	data []byte
	seq  uint64
}

func (msg *seqMessage) Data() []byte {
	return msg.data
}

func (msg *seqMessage) DiscardPayload() {
	msg.data = nil
}
