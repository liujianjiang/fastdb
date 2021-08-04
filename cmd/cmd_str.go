package cmd

import (
	"errors"
	"fastdb"
	"fmt"

	"github.com/tidwall/redcon"
)

// ErrSyntaxIncorrect incorrect err
var ErrSyntaxIncorrect = errors.New("syntax err")
var okResult = redcon.SimpleString("OK")

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}

func set(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("set")
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil {
		res = okResult
	}
	return
}

func get(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("get")
		return
	}
	key := args[0]
	var val []byte
	if val, err = db.Get([]byte(key)); err == nil {
		res = string(val)
	}
	return
}

func strLen(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strlen")
		return
	}
	length := db.StrLen([]byte(args[0]))
	res = redcon.SimpleInt(length)
	return
}

func strExists(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strexists")
		return
	}
	if exists := db.StrExists([]byte(args[0])); exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func strRem(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strrem")
		return
	}
	if err = db.StrRem([]byte(args[0])); err == nil {
		res = okResult
	}
	return
}

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
}
