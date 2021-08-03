package cmd

import (
	"fastdb"
	"fmt"
	"log"
	"strings"

	"github.com/tidwall/redcon"
)

type ExecCmdFunc func(*fastdb.FastDb, []string) (interface{}, error)

var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type Server struct {
	server *redcon.Server
	db     *fastdb.FastDb
}

// 创建服务
func NewServer(config fastdb.Config) (*Server, error) {
	db, err := fastdb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

// 监听服务
func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			log.Printf("accept: %s", string(cmd.Args[0]))
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	if err := s.server.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()

	command := strings.ToLower(string(cmd.Args[0]))
	exec, exist := ExecCmd[command]
	if !exist {
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
		return
	}
	args := make([]string, 0, len(cmd.Args)-1)
	for i, bytes := range cmd.Args {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	reply, err := exec(s.db, args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)
}
