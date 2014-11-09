package boltdb

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/brettbuddin/victor/pkg/store"
)

func init() {
	fmt.Println("loading")

	dbPath := os.Getenv("VICTOR_STORAGE_PATH")

	// type InitFunc func() Adapter
	store.Register("boltdb", func() store.Adapter {
		db, err := bolt.Open(dbPath, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}

		return &BoltStore{
			db:            db,
			defaultBucket: "victor",
		}
	})
}

type BoltStore struct {
	defaultBucket string
	db            *bolt.DB
}

// type Adapter interface {
// 	Get(string) (string, bool)
// 	Set(string, string)
// 	Delete(string)
// 	All() map[string]string
// }

// get ':' split key, return bucket / key. Only the first ':' is observed, so
//
// "pattern:asdf:begs" is:
//
//   bucket: pattern
//	 key:    asdf:begs
//
func (s *BoltStore) splitKey(key string) (string, string) {
	vals := strings.SplitN(key, ":", 1)
	if len(vals) > 1 {
		return vals[0], vals[1]
	} else {
		return s.defaultBucket, vals[0]
	}
}

func (s *BoltStore) Get(bucketKey string) (string, bool) {
	bucket, key := s.splitKey(bucketKey)

	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		val = b.Get([]byte(key))
		fmt.Printf("loaded '%s' from '%s:%s'\n", val, bucket, key)
		return nil
	})

	if err != nil {
		log.Println("[boltdb Get] error getting", bucketKey, "-", err)
	}

	if val != nil {
		return string(val), true
	} else {
		return "", false
	}
}

func (s *BoltStore) Set(bucketKey string, val string) {
	bucket, key := s.splitKey(bucketKey)

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put([]byte(key), []byte(val))
		return err
	})

	if err != nil {
		log.Println("[boltdb Set] error setting", bucketKey, "to", val, "-", err)
	}
}

func (s *BoltStore) Delete(bucketKey string) {
	bucket, key := s.splitKey(bucketKey)

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Delete([]byte(key))
		return err
	})

	if err != nil {
		log.Println("[boltdb Delete] error deleting", bucketKey, "-", err)
	}
}

func (s *BoltStore) All() map[string]string {
	return map[string]string{}
}
