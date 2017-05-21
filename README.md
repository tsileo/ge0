# ge0

`ge0` is a geospatial data store designed to store your own previous locations, extensible via Lua.

## Features

 - Keep track of you location (lat/long points)
 - Execute Lua scripts (using [gluapp framework](https://github.com/tsileo/gluapp)) on new data points (call external services, store additional indexes)
 - Create complex queries in Lua
 - Built-in JSON key-value store
 - Built-in geospatial index using geohashes
 - Built-in offline reverse geocoding API

## Quickstart

```shell
$ mkdir myapp
$ echo "app.response:write('hello myapp')" > myapp/app.lua
$ ge0 myapp
```

```shell
$ curl http://localhost:8010/app
hello myapp
```

### Reversegeo index

The index is built on the `cities1000.zip` (all cities with a population > 1000) dump from [GeoNames](http://www.geonames.org/).

You can download it [here](all cities with a population > 1000). Once you have unzipped the file, run:

```shell
$ ge0 -build-reversegeo-index -path-cities1000txt=/path/to/cities1000.txt /path/to/your/app
```

## API

### GET /api/reversegeo

Offline reverse geocoding, using the "cities" Places index (from Geonames Cities 1000 dataset).

Parameters: `lat`, `lng` and `precision` (optional, in meters).

```
$ curl http://localhost:8010/api/reversegeo?lat=1.0&lng=1.0
```

## Applications built for ge0

 - [Went there](https://github.com/tsileo/went-there): the app I'm using to track my location.
 - Add yours!
