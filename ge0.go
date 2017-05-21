package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/yuin/gopher-lua"

	"a4.io/gluapp"
	"a4.io/rawgeo"

	"github.com/tsileo/ge0/pkg/kv"
	"github.com/tsileo/ge0/pkg/reversegeo"
	"github.com/tsileo/ge0/pkg/timeseries"
)

// XXX(tsileo): backup CLI that create a gzip

func ServerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "ge0")
		next.ServeHTTP(w, r)
		// TODO(tsileo): move this a app wrapper code or in Gluap (along with a SetHeaders hook)?
		// t := time.Now()
		// w.Header().Set("X-Ge0-Response-Time-Nano", strconv.Itoa(int(time.Since(t))))
	})
}

func APIMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

// TODO(tsileo): use handler from gorilla/handlers
func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept")
		w.Header().Set("Access-Control-Allow-Methods", "POST, PATCH, GET, OPTIONS, DELETE, PUT")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	var noApp, reloadReverseGeo bool
	var pathCities1000, listen string
	flag.BoolVar(&noApp, "no-app", false, "Disable the Lua app")
	flag.BoolVar(&reloadReverseGeo, "build-reversegeo-index", false, "Build the reversegeo database from the cities1000 dataset")
	flag.StringVar(&pathCities1000, "path-cities1000txt", "cities1000.txt", "Path to the cities1000.txt file for reloading the reversegeo db")
	flag.StringVar(&listen, "listen", ":8010", "host:port for the HTTP server")
	flag.Parse()
	appPath := flag.Arg(0)
	if !noApp && appPath == "" {
		log.Printf("missing app path")
		return
	}
	log.Printf("appPath=%v", appPath)
	dataPath := filepath.Join(appPath, "data")

	// XXX(tsileo): check if exists
	os.MkdirAll(dataPath, 0700)

	stop := make(chan os.Signal)

	signal.Notify(stop, os.Interrupt)
	// TODO(tsileo): YAML config and DBs per app
	db, err := rawgeo.New(filepath.Join(dataPath, "reversegeo.geoindex.db"))
	if err != nil {
		panic(err)
	}
	p, err := kv.New(filepath.Join(dataPath, "cities.kv.db"))
	if err != nil {
		panic(err)
	}
	// TODO(tsileo): use middleware to set Server: ge0 and X-Ge0-App: <app name> and request ID
	// TODO(tsileo): logs in Lua (to merge in Gluapp)
	ts, err := timeseries.New(filepath.Join(dataPath, "timeseries.db"))
	if err != nil {
		panic(err)
	}
	rg, err := reversegeo.New(db, p)
	if err != nil {
		panic(err)
	}
	if reloadReverseGeo {
		log.Println("Buidling the reverse geo index... (This may take few minutes)")
		if err := rg.InitialLoading(pathCities1000); err != nil {
			panic(err)
		}
		log.Println("Done")
	}

	r := mux.NewRouter()
	if !noApp {
		log.Printf("Lua app disabled")
		setupApp := func(L *lua.LState) error {
			// FIXME(tsileo): set "nano" time.UnixNano and a custom log func (and requestID?)
			ts.SetupLua(L)
			rg.SetupLua(L)
			return nil
		}

		app, err := gluapp.NewApp(&gluapp.Config{Path: filepath.Join(appPath, "app"), SetupState: setupApp})
		if err != nil {
			panic(err)
		}

		r.PathPrefix("/app").Handler(app)
	}

	api := mux.NewRouter()

	rg.SetupAPI(api)
	ts.SetupAPI(api)

	r.PathPrefix("/api").Handler(CorsMiddleware(APIMiddleware(api)))

	router := handlers.LoggingHandler(os.Stdout, r)
	http.Handle("/", ServerMiddleware(router))
	h := &http.Server{Addr: listen, Handler: nil}

	go func() {
		log.Printf("Listening on %s", listen)
		if err := h.ListenAndServe(); err != nil {
			log.Printf("failed to start server: %s", err)
		}
	}()
	<-stop

	log.Println("Shutting down the server...")

	h.Shutdown(context.Background())
	rg.Close()
	ts.Close()
}
