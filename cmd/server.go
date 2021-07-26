package cmd

import (
	"fastdb"
	"log"

	"github.com/tidwall/redcon"
)

type Server struct {
	server *redcon.Server
	db     *fastdb.FastDb
}

// NewServer create a new rosedb server.
func NewServer(config fastdb.Config) (*Server, error) {
	db, err := fastdb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

// Listen listen the server.
func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			//s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	if err := svr.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}
