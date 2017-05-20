package places

import (
	"os"
	"reflect"
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestPlaces(t *testing.T) {
	ps, err := New("places_test", "test")
	defer func() {
		ps.Close()
		os.RemoveAll("places_test")
	}()
	check(err)
	p := &Place{
		Lat: 1.0,
		Lng: 3.0,
	}
	_id, err := ps.Insert(p)
	check(err)
	p2, err := ps.Get(_id.String())
	check(err)
	if !reflect.DeepEqual(p, p2) {
		t.Errorf("f")
	}
}
