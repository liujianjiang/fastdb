package cmd

import (
	"fastdb"

	"github.com/tidwall/redcon"
)

func hSet(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("hset")
		return
	}
	var count int
	if count, err = db.HSet([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func hGet(db *fastdb.FastDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	val := db.HGet([]byte(args[0]), []byte(args[1]))
	if len(val) == 0 {
		res = nil
	} else {
		res = string(val)
	}
	return
}

func init() {
	addExecCommand("hset", hSet)
	addExecCommand("hget", hGet)

}
