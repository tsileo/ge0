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

// XXX(tsileo): filter by feature class/feature code to only restrict to cities
func (rg *ReverseGeo) InitialLoading(pathCities1000 string) error {
	p := rg.kv
	db := rg.rawgeo
	file, err := os.Open(pathCities1000)
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
	if place == nil {
		place = &Place{
			Lat: lat,
			Lng: lng,
		}
	}
	httputil.WriteJSON(w, place)
}

func (rg *ReverseGeo) SetupAPI(router *mux.Router) {
	router.HandleFunc("/api/reversegeo", rg.apiReverseGeo)
}
