# ge0

Offline reverse geolocation HTTP API.

## Quickstart

The index is built on the `cities1000.zip` (all cities with a population > 1000) dump from [GeoNames](http://www.geonames.org/).

You can download it [here](all cities with a population > 1000). Once you have unzipped the file, run:

```shell
$ ge0 -build-reversegeo-index -path-cities1000txt=/path/to/cities1000.txt /data/dir
```

Once the index is built, you can just run:

```shell
$ ge0 /data/dir
```

## API

### GET /api/reversegeo

Offline reverse geocoding, using the "cities" Places index (from Geonames Cities 1000 dataset).

Returns the nearest city, `data` can be `null` if no city is found with the given precision.

Parameters: `lat`, `lng` and `precision` (optional, in meters, default is 5km).

```
$ curl http://localhost:8010/api/reversegeo?lat=1.0&lng=1.0
```
