package store

import (
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
)

// Store defines interface for persisting messages.
type Store interface {
	TopicStore(topicName string) (TopicStore, error)
	Close() error
}

// NewBadgerStore returns a store instance backed by badger db.
func NewBadgerStore(path string, maxCacheSize int64) (Store, error) {
	if maxCacheSize == 0 {
		maxCacheSize = 1 << 30 // 1 GB = Badger default
	}
	db, err := badger.Open(
		badger.DefaultOptions(path).
			WithMaxCacheSize(maxCacheSize).
			WithNumVersionsToKeep(1).
			WithNumLevelZeroTables(1).
			WithNumLevelZeroTablesStall(2),
	)
	if err != nil {
		return nil, err
	}

	store := &badgerStore{
		db:     db,
		stopGC: make(chan struct{}),
	}
	store.startGC()

	return store, nil
}

type badgerStore struct {
	db     *badger.DB
	stopGC chan struct{}
}

func (store *badgerStore) TopicStore(topicName string) (TopicStore, error) {
	tStore := &badgerTopicStore{
		db:         store.db,
		topic:      []byte(topicName + ":"),
		newMessage: sync.NewCond(&sync.Mutex{}),
	}

	if err := tStore.init(); err != nil {
		return nil, err
	}
	return tStore, nil
}

func (store *badgerStore) startGC() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-store.stopGC:
			case <-ticker.C:
			again:
				err := store.db.RunValueLogGC(0.5)
				if err == nil {
					goto again
				}
			}
		}
	}()
}

func (store *badgerStore) Close() error {
	close(store.stopGC)
	return store.db.Close()
}
