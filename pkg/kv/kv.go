package kv

import (
	"encoding/json"
	_ "fmt"
	"os"
	"time"

	"a4.io/blobstash/pkg/docstore/id"

	ckv "github.com/cznic/kv"
)

type KV struct {
	db   *ckv.DB
	path string
}

func New(path string) (*KV, error) {
	createOpen := ckv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = ckv.Create
	}
	kvdb, err := createOpen(path, &ckv.Options{})
	if err != nil {
		return nil, err
	}
	return &KV{
		db:   kvdb,
		path: path,
	}, nil
}

func (kv *KV) Close() error {
	return kv.db.Close()
}

func (kv *KV) Get(id string, res interface{}) error {
	js, err := kv.db.Get(nil, []byte(id))
	if err != nil {
		return err
	}

	if err := json.Unmarshal(js, res); err != nil {
		return err
	}

	return nil
}

func (kv *KV) Insert(data interface{}) (*id.ID, error) {
	_id, err := id.New(time.Now().UTC().UnixNano())
	if err != nil {
		return nil, err
	}

	js, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	if err := kv.db.Set([]byte(_id.String()), js); err != nil {
		return nil, err
	}

	return _id, nil
}
