package reversegeo

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pariz/gountries"
	"github.com/tsileo/ge0/pkg/kv"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/rawgeo"
)

// TODO(tsile): explain how to download cities1000.txt and give a setup script

var data = gountries.New()

type Place struct {
	Lng  float64                `json:"lng"`
	Lat  float64                `json:"lat"`
	Data map[string]interface{} `json:"data"`
}

type Location struct {
	ID                 string  `json:"id"`          // 0
	CityName           string  `json:"city_name"`   // 1
	Lat                float64 `json:"lat"`         // 4
	Lon                float64 `json:"lon"`         // 5
	CountryCode        string  `json:"cc"`          // 8
	AdminCode          string  `json:"admin"`       // 10
	CountrySubdivision string  `json:"subdivision"` // 11

	CountryName     string `json:"country_name"`
	SubdivisionName string `json:"subdivison_name"`
	Name            string `json:"name"`
}

func (l *Location) ToPlace() *Place {
	return &Place{
		Lat: l.Lat,
		Lng: l.Lon,
		Data: map[string]interface{}{
			"city_name":        l.CityName,
			"cc":               l.CountryCode,
			"admin":            l.AdminCode,
			"subdivision":      l.CountrySubdivision,
			"country_name":     l.CountryName,
			"subdivision_name": l.SubdivisionName,
			"name":             l.Name,
		},
	}
}

// https://github.com/hexorx/countries/tree/master/lib
// source => https://anonscm.debian.org/cgit/pkg-isocodes/iso-codes.git/tree/iso_3166-2

// XXX(tsileo): filter by feature class/feature code to only restrict to cities

func parseLocation(p *kv.KV, db *rawgeo.RawGeo) error {
	file, err := os.Open("cities1000.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := scan.Text()
		s := strings.Split(line, "\t")
		if len(s) < 19 {
			continue
		}
		lat, err := strconv.ParseFloat(s[4], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(s[5], 64)
		if err != nil {
			return err
		}
		loc := &Location{
			ID:                 s[0],
			CityName:           s[1],
			Lat:                lat,
			Lon:                lon,
			CountryCode:        s[8],
			AdminCode:          s[10], // admin
			CountrySubdivision: s[11], // admin2
		}
		country, _ := data.FindCountryByAlpha(loc.CountryCode)

		loc.CountryName = country.Name.Common
		for _, subdiv := range country.SubDivisions() {
			if subdiv.Code == loc.CountrySubdivision {
				loc.SubdivisionName = subdiv.Name
			}
		}

		// FIXME(ts): add the state for the US
		if loc.SubdivisionName != "" {
			loc.Name = fmt.Sprintf("%s, %s, %s", loc.CityName, loc.SubdivisionName, loc.CountryName)
		} else {
			loc.Name = fmt.Sprintf("%s, %s", loc.CityName, loc.CountryName)
		}
		_id, err := p.Insert(loc.ToPlace())
		if err != nil {
			return err
		}

		// locations[loc.ID] = loc
		p := &rawgeo.Point{
			ID:  _id.String(),
			Lat: lat,
			Lng: lon,
		}
		if err := db.Index(p); err != nil {
			if err == rawgeo.ErrInvalidLatLong {
				continue
			}
			return err
		}
	}
	return nil
}

type ReverseGeo struct {
	rawgeo *rawgeo.RawGeo
	kv     *kv.KV
}

func New(rg *rawgeo.RawGeo, kv *kv.KV) (*ReverseGeo, error) {
	return &ReverseGeo{
		rawgeo: rg,
		kv:     kv,
	}, nil
}

func (rg *ReverseGeo) Close() error {
	rg.rawgeo.Close()
	rg.kv.Close()
	return nil
}

func (rg *ReverseGeo) Query(lat, lng float64, prec int) (*Place, error) {
	res, err := rg.rawgeo.Query(lat, lng, float64(prec)) // 40m
	if err != nil {
		return nil, err
	}
	if res != nil && len(res) > 0 {
		p := &Place{}
		if err := rg.kv.Get(res[0].ID, p); err != nil {
			return nil, err
		}
		return p, nil
	}
	return nil, nil
}

func (rg *ReverseGeo) SetupLua() func(*lua.LState) int {
	return func(L *lua.LState) int {
		// Setup the "reversegeo" module
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"reversegeo": func(L *lua.LState) int {
				tbl := L.CheckTable(1)
				if tbl == nil {
					return 1
				}
				lat := float64(tbl.RawGetH(lua.LString("lat")).(lua.LNumber))
				lng := float64(tbl.RawGetH(lua.LString("lng")).(lua.LNumber))
				place, err := rg.Query(lat, lng, 10000)
				if err != nil {
					panic(err)
				}
				res := L.CreateTable(0, 3)
				res.RawSetH(lua.LString("lat"), lua.LNumber(place.Lat))
				res.RawSetH(lua.LString("lng"), lua.LNumber(place.Lng))
				res.RawSetH(lua.LString("data"), luautil.InterfaceToLValue(L, place.Data))
				L.Push(res)
				return 1
			},
		})
		L.Push(mod)
		return 1
	}
}

func (rg *ReverseGeo) apiReverseGeo(w http.ResponseWriter, r *http.Request) {
	q := httputil.NewQuery(r.URL.Query())
	lat, err := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	if err != nil {
		panic(err)
	}
	lng, err := strconv.ParseFloat(r.URL.Query().Get("lng"), 64)
	if err != nil {
		panic(err)
	}
	precision, err := q.GetInt("precision", 5000, 50000)
	if err != nil {
		panic(err)
	}

	place, err := rg.Query(lat, lng, precision)
	if err != nil {
		panic(err)
	}
	httputil.WriteJSON(w, place)
}

func (rg *ReverseGeo) SetupAPI(router *mux.Router) {
	router.HandleFunc("/api/reversegeo", rg.apiReverseGeo)
}