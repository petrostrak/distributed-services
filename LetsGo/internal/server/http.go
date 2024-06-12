package server

import "net/http"

type HTTPServer struct {
	Log *Log
}

func newHTTPServer() *HTTPServer {
	return &HTTPServer{
		NewLog(),
	}
}

func NewHTTPServer(addr string) *http.Server {
	srv := newHTTPServer()
	r := http.NewServeMux()

	r.HandleFunc("POST /", srv.handleProduce)
	r.HandleFunc("GET /", srv.handleConsume)

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *HTTPServer) handleProduce(w http.ResponseWriter, r *http.Request) {}

func (s *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {}
