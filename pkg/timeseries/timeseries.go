package timeseries

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cznic/kv"
	"github.com/gorilla/mux"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/httputil"
)

// Define namespaces for raw key sorted in db.
const (
	_ byte = iota
	KvKeyIndex
	KvItem
)

// KvType for meta serialization
const KvType = "kv"

type Flag byte

const (
	Unknown Flag = iota
	Deleted
	HashOnly
	DataOnly
	HashAndData
)

var ErrNotFound = errors.New("vkv: key does not exist")

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key string `json:"key"`

	// FIXME(tsileo): turn this into a []*VkvEntry
	Versions []*KeyValue `json:"versions"`
}

// NextKey returns the next key for lexigraphical (key = NextKey(lastkey))
func NextKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return string(bkey)
}

// PextKey returns the next key for lexigraphical (key = PextKey(lastkey))
func PrevKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]--
		if bkey[i] != 255 {
			break
		}
	}
	return string(bkey)
}

type DB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

func encodeKey(key []byte, version int) []byte {
	versionbyte := make([]byte, 8)
	binary.BigEndian.PutUint64(versionbyte, uint64(version))
	k := make([]byte, len(key)+13)
	k[0] = KvItem
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	copy(k[5:], key)
	copy(k[5+len(key):], versionbyte)
	return k
}

// Extract the index from the raw key
func decodeKey(key []byte) (string, int) {
	klen := int(binary.LittleEndian.Uint32(key[1:5]))
	index := int(binary.BigEndian.Uint64(key[len(key)-8:]))
	member := make([]byte, klen)
	copy(member, key[5:5+klen])
	return string(member), index
}

func encodeMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

// Put updates the value for the given version associated with key,
// if version == -1, version will be set to time.Now().UTC().UnixNano().
func (db *DB) Put(key string, data []byte, version int) (*KeyValue, error) {
	if version < 1 {
		version = int(time.Now().UTC().UnixNano())
	}
	bkey := []byte(key)
	kmember := encodeKey(bkey, version)
	kv := &KeyValue{
		Data:    data,
		Version: version,
		Key:     key,
	}
	if err := db.db.Set(kmember, data); err != nil {
		return nil, err
	}
	if err := db.db.Set(encodeMeta(KvKeyIndex, bkey), kmember); err != nil {
		return nil, err
	}
	return kv, nil
}

func (db *DB) Check(key string) (bool, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return false, err
	}
	if len(exists) == 0 {
		return false, nil
	}
	return true, nil
}

// Get returns the latest value for the given key,
// if version < 1, the latest version will be returned.
func (db *DB) Get(key string, version int) (*KeyValue, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, fmt.Errorf("index key lookup failed: %v", err)
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	var k []byte
	if version < 1 {
		k, err = db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
		if err != nil {
			return nil, fmt.Errorf("failed to get max version: %v", err)
		}
		version = int(binary.BigEndian.Uint64(k[len(k)-8:]))
	} else {
		k = encodeKey(bkey, version)
	}
	val, err := db.db.Get(nil, k)
	if err != nil {
		return nil, fmt.Errorf("failed to get key \"%s\": %v", encodeKey(bkey, version), err)
	}
	kv := &KeyValue{Key: key, Version: version, Data: val}

	return kv, nil
}

// Return the versions in reverse lexicographical order
func (db *DB) DataPoints(key string, start, end, limit int) (*KeyValueVersions, int, error) { // TODO(tsileo): return a "new start" int like in Keys
	var nstart int
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, nstart, err
	}
	if len(exists) == 0 {
		return nil, nstart, ErrNotFound
	}
	if start < 1 {
		start = int(time.Now().UTC().UnixNano())
	}
	enum, _, err := db.db.Seek(encodeKey(bkey, start))
	if err != nil {
		return nil, nstart, err
	}
	endBytes := encodeKey(bkey, end)
	i := 1
	skipOnce := true
	eofOnce := true
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			if eofOnce {
				enum, err = db.db.SeekLast()
				if err != nil {
					return nil, nstart, err
				}
				eofOnce = false
				continue
			}
			break
		}
		if bytes.Equal(k, encodeKey(bkey, start)) {
			continue
		}
		if bytes.Compare(k, endBytes) < 1 || len(endBytes) < 8 || !bytes.HasPrefix(k, endBytes[0:len(endBytes)-8]) || (limit > 0 && i > limit) {
			if skipOnce {
				skipOnce = false
				continue
			}
			return res, nstart, nil
		}
		_, index := decodeKey(k)
		kv := &KeyValue{Key: key, Version: index, Data: v}
		res.Versions = append(res.Versions, kv)
		nstart = index
		i++
	}
	return res, nstart, nil
}

// Return a lexicographical range
func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, string, error) {
	var next string
	res := []*KeyValue{}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(start)))
	if err != nil {
		return nil, next, fmt.Errorf("initial seek error: %v", err)
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 1
	for {
		k, k2, err := enum.Next()
		if err == io.EOF {
			break
		}
		if k[0] != KvKeyIndex || bytes.Compare(k, endBytes) > 0 || (limit > 0 && i > limit) {
			return res, next, nil
		}
		version := int(binary.BigEndian.Uint64(k2[len(k2)-8:]))
		val, err := db.db.Get(nil, k2)
		if err != nil {
			return nil, next, err
		}
		kv := &KeyValue{Key: string(k[1:]), Version: version, Data: val}
		res = append(res, kv)
		next = NextKey(kv.Key)
		i++
	}
	return res, next, nil
}

// Return a lexicographical range
func (db *DB) ReverseKeys(start, end string, limit int) ([]*KeyValue, string, error) {
	var prev string
	res := []*KeyValue{}
	startBytes := encodeMeta(KvKeyIndex, []byte(start))
	enum, _, err := db.db.Seek(startBytes)
	if err != nil {
		return nil, prev, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 1
	skipOnce := true
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if k[0] != KvKeyIndex || bytes.Compare(k, startBytes) > 0 || bytes.Compare(k, endBytes) < 0 || (limit > 0 && i > limit) {
			if skipOnce {
				skipOnce = false
				continue
			}
			return res, prev, nil
		}
		version := int(binary.BigEndian.Uint64(v[len(v)-8:]))
		val, err := db.db.Get(nil, v)
		if err != nil {
			return nil, prev, err
		}
		kv := &KeyValue{Key: string(k[1:]), Version: version, Data: val}
		res = append(res, kv)
		prev = PrevKey(kv.Key)
		i++
	}
	return res, prev, nil
}

type KeyValue struct {
	Key     string
	Version int
	Data    []byte
}

type luaTimeSeries struct {
	db   *DB
	name string
}

func LuaTimeSeries(L *lua.LState, db *DB) (*lua.LUserData, error) {
	lts := &luaTimeSeries{
		db: db,
	}
	mt := L.NewTypeMetatable("timeseries")
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"insert": tsInsert,
	}))
	ud := L.NewUserData()
	ud.Value = lts
	L.SetMetatable(ud, L.GetTypeMetatable("timeseries"))
	return ud, nil
}

func checkLuaTimeSeries(L *lua.LState) *luaTimeSeries {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*luaTimeSeries); ok {
		return v
	}
	L.ArgError(1, "timeseries expected")
	return nil
}

func tsInsert(L *lua.LState) int {
	ts := checkLuaTimeSeries(L)
	if ts == nil {
		return 1
	}
	var at int64
	at = -1
	if L.GetTop() == 3 {
		at = int64(L.ToInt64(3))
	}
	val := L.CheckTable(2)
	if val == nil {
		return 1
	}

	js, err := json.Marshal(luautil.TableToMap(val))
	if err != nil {
		panic(err)
	}
	if _, err := ts.db.Put(ts.name, js, int(at)); err != nil {
		panic(err)
	}
	return 0
}

func tsRange(L *lua.LState) int {
	ts := checkLuaTimeSeries(L)
	if ts == nil {
		return 1
	}
	start := L.ToInt64(2)
	end := L.ToInt64(3)
	limit := 50
	if L.GetTop() == 4 {
		limit = int(L.ToInt64(4))
	}
	kv, cursor, err := ts.db.DataPoints(ts.name, int(start), int(end), limit)
	if err != nil {
		panic(err)
	}
	results := L.CreateTable(len(kv.Versions), 0)
	for _, kv := range kv.Versions {
		tbl := L.CreateTable(0, 2)
		tbl.RawSetH(lua.LString("data"), luautil.FromJSON(L, kv.Data))
		tbl.RawSetH(lua.LString("time"), lua.LNumber(kv.Version))
		results.Append(tbl)
	}
	L.Push(results)
	L.Push(lua.LNumber(cursor))
	return 2
}

func (db *DB) SetupLua() func(*lua.LState) int {
	return func(L *lua.LState) int {
		// Setup the Lua meta table the http (client) user-defined type
		mtHTTP := L.NewTypeMetatable("timeseries")
		methods := map[string]lua.LGFunction{
			"insert": tsInsert,
			"range":  tsRange,
		}
		L.SetField(mtHTTP, "__index", L.SetFuncs(L.NewTable(), methods))

		// Setup the "http" module
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"new": func(L *lua.LState) int {
				lts := &luaTimeSeries{
					db:   db,
					name: L.ToString(1),
				}
				ud := L.NewUserData()
				ud.Value = lts
				L.SetMetatable(ud, L.GetTypeMetatable("timeseries"))
				L.Push(ud)
				return 1
			},
		})
		L.Push(mod)
		return 1
	}
}

func (db *DB) apiTimeSeries(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	q := httputil.NewQuery(r.URL.Query())
	switch r.Method {
	case "GET":
		start, err := q.GetIntDefault("start", -1)
		if err != nil {
			panic(err)
		}
		end, err := q.GetIntDefault("end", 0)
		if err != nil {
			panic(err)
		}
		kv, cursor, err := db.DataPoints(vars["name"], start, end, 51)
		if err != nil {
			panic(err)
		}
		res := []map[string]interface{}{}
		for _, dp := range kv.Versions {
			data := map[string]interface{}{}
			if err := json.Unmarshal(dp.Data, &data); err != nil {
				panic(err)
			}
			res = append(res, map[string]interface{}{
				"time": dp.Version,
				"data": data,
			})
		}
		var more bool
		if len(res) > 50 {
			more = true
		}
		httputil.WriteJSON(w, map[string]interface{}{
			"timeseries": map[string]interface{}{
				"name": vars["name"],
			},
			"data": res,
			"pagination": map[string]interface{}{
				"cursor":   cursor,
				"has_more": more,
			},
		})
	case "POST":
		data := map[string]interface{}{}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			panic(err)
		}
		js, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		kv, err := db.Put(vars["name"], js, -1)
		if err != nil {
			panic(err)
		}
		w.Header().Add("X-Ge0-TimeSeries-Time", strconv.Itoa(kv.Version))
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (db *DB) SetupAPI(router *mux.Router) {
	router.HandleFunc("/api/timeseries/{name}", db.apiTimeSeries)
}
