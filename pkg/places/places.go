package places

import (
	"encoding/json"
	_ "fmt"
	"os"
	"time"

	"a4.io/blobstash/pkg/docstore/id"

	"github.com/cznic/kv"
)

type Places struct {
	db   *kv.DB
	path string
}

type Place struct {
	ID   *id.ID                 `json:"-"`
	Lng  float64                `json:"lng"`
	Lat  float64                `json:"lat"`
	Data map[string]interface{} `json:"data"`
}

func New(path string) (*Places, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &Places{
		db:   kvdb,
		path: path,
	}, nil
}

func (ps *Places) Close() error {
	return ps.db.Close()
}

func (ps *Places) Get(id string) (*Place, error) {
	js, err := ps.db.Get(nil, []byte(id))
	if err != nil {
		return nil, err
	}

	p := &Place{}
	if err := json.Unmarshal(js, p); err != nil {
		return nil, err
	}

	return p, err
}

func (ps *Places) Insert(p *Place) (*id.ID, error) {
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
