package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pariz/gountries"

	"a4.io/rawgeo"
)

// FIXME(tsileo): use Places API instead of `locations = map[string]...`, and use id.ID from BlobStash Docstore (maybe extract it? along with Luatuil?)
// TODO(tsile): explain how to download cities1000.txt and give a setup script

var data = gountries.New()

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

func (l *Location) ToPlace() *place {
	return &place{
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

func parseLocation(p *places, db *rawgeo.RawGeo) error {
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

func main() {
	n := time.Now()
	db, err := rawgeo.New("data/cities.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	p, err := newPlaces("data/cities.places.db", "cities")
	if err != nil {
		panic(err)
	}
	defer p.Close()
	// if err := parseLocation(p, db); err != nil {
	// panic(err)
	// }
	took := time.Since(n)
	fmt.Printf("loading took %v\n", took)

	n = time.Now()
	// Query should return Austin TX
	// res, err := db.Query(48.26127189, 4.0871129, 1000)
	res, err := db.Query(30.26715, -97.74306, 40) // 40m
	took = time.Since(n)
	fmt.Printf("query took %v\n,res=%v/err=%v", took, res, err)
	if res != nil && len(res) > 0 {
		p2, err := p.Get(res[0].ID)
		if err != nil {
			panic(err)
		}

		fmt.Printf("res=%+v", p2)
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
