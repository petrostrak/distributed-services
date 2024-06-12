package main

import (
	"log"
	"log/slog"

	"github.com/petrostrak/proglog/LetsGo/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":3000")
	slog.Info("server running on", "port", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
