package main

import (
	"encoding/json"
	_ "fmt"
	"os"
	"time"

	"a4.io/blobstash/pkg/docstore/id"

	"github.com/cznic/kv"
)

type places struct {
	db   *kv.DB
	path string
	name string
}

type place struct {
	ID   *id.ID                 `json:"-"`
	Lng  float64                `json:"lng"`
	Lat  float64                `json:"lat"`
	Data map[string]interface{} `json:"data"`
}

func newPlaces(path, name string) (*places, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &places{
		db:   kvdb,
		path: path,
	}, nil
}

func (ps *places) Close() error {
	return ps.db.Close()
}

func (ps *places) Get(id string) (*place, error) {
	js, err := ps.db.Get(nil, []byte(id))
	if err != nil {
		return nil, err
	}

	p := &place{}
	if err := json.Unmarshal(js, p); err != nil {
		return nil, err
	}

	return p, err
}

func (ps *places) Insert(p *place) (*id.ID, error) {
	_id, err := id.New(time.Now().UTC().UnixNano())
	if err != nil {
		return nil, err
	}

	js, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	if err := ps.db.Set([]byte(_id.String()), js); err != nil {
		return nil, err
	}

	return _id, nil
}
