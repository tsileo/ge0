package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	"a4.io/gluapp"
	"github.com/gorilla/mux"
	"github.com/tsileo/ge0/pkg/kv"
	"github.com/tsileo/ge0/pkg/reversegeo"
	"github.com/tsileo/ge0/pkg/timeseries"
	"github.com/yuin/gopher-lua"

	"a4.io/rawgeo"
)

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
	stop := make(chan os.Signal)

	signal.Notify(stop, os.Interrupt)
	// TODO(tsileo): YAML config and DBs per app
	db, err := rawgeo.New("data/cities.db")
	if err != nil {
		panic(err)
	}
	p, err := kv.New("data/cities.places.db")
	if err != nil {
		panic(err)
	}
	// TODO(tsileo): use middleware to set Server: ge0 and X-Ge0-App: <app name> and request ID
	// TODO(tsileo): logs in Lua (to merge in Gluapp)
	ts, err := timeseries.New("ts.db")
	if err != nil {
		panic(err)
	}
	rg, err := reversegeo.New(db, p)
	if err != nil {
		panic(err)
	}

	setupApp := func(L *lua.LState) error {
		// FIXME(tsileo): set "nano" time.UnixNano and a custom log func (and requestID?)
		// TODO(tsileo): ts.SetupLua(L) instead
		L.PreloadModule("timeseries", ts.SetupLua())
		L.PreloadModule("reversegeo", rg.SetupLua())
		return nil
	}
	app, err := gluapp.NewApp(&gluapp.Config{Path: "testapp", SetupState: setupApp})
	if err != nil {
		panic(err)
	}

	// kv (for counter?), rawgeo (and id stored with counter)
	r := mux.NewRouter()

	api := mux.NewRouter()
	rg.SetupAPI(api)
	ts.SetupAPI(api)

	// XXX(tsileo): backup CLI that create a gzip

	r.PathPrefix("/app").Handler(app)
	r.PathPrefix("/api").Handler(CorsMiddleware(APIMiddleware(api)))
	http.Handle("/", ServerMiddleware(r))
	h := &http.Server{Addr: ":8010", Handler: nil}

	go func() {
		if err := h.ListenAndServe(); err != nil {
			log.Printf("failed to start server: %s", err)
		}
	}()
	<-stop

	log.Println("\nShutting down the server...")

	h.Shutdown(context.Background())
	rg.Close()
	ts.Close()
}
