package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"a4.io/rawgeo"

	"github.com/tsileo/ge0/pkg/kv"
	"github.com/tsileo/ge0/pkg/reversegeo"
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
	var reloadReverseGeo bool
	var pathCities1000, listen string
	flag.BoolVar(&reloadReverseGeo, "build-reversegeo-index", false, "Build the reversegeo database from the cities1000 dataset")
	flag.StringVar(&pathCities1000, "path-cities1000txt", "cities1000.txt", "Path to the cities1000.txt file for reloading the reversegeo db")
	flag.StringVar(&listen, "listen", ":8010", "host:port for the HTTP server")
	flag.Parse()
	dataPath := filepath.Join(flag.Arg(0), "data")
	log.Printf("dataPath=%v", dataPath)

	stop := make(chan os.Signal)

	signal.Notify(stop,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	// TODO(tsileo): YAML config and DBs per app
	db, err := rawgeo.New(filepath.Join(dataPath, "reversegeo.geoindex.db"))
	if err != nil {
		panic(err)
	}
	p, err := kv.New(filepath.Join(dataPath, "cities.kv.db"))
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
	api := mux.NewRouter()

	rg.SetupAPI(api)
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
}
