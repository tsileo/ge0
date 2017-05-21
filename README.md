# ge0

`ge0` is a geospatial data store designed to store your own previous locations, extensible via Lua.

## Features

 - Keep track of you location (lat/long points)
 - Execute Lua scripts on new data points (call external services, store additional indexes)
 - Create complex queries in Lua
 - Built-in JSON key-value store
 - Built-in geospatial index using geohashes
 - Built-in offline reverse geocoding API

## API

### GET /api/reversegeo

Offline reverse geocoding, using the "cities" Places index (from Geonames Cities 1000 dataset).

Parameters: `lat`, `lng` and `precision` (optional, in meters).

```
$ curl http://localhost:8010/api/reversegeo?lat=1.0&lng=1.0
```
