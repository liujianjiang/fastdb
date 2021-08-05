package fastdb

import (
	"log"
	"testing"
)

var key = "myhash"

func TestFastDB_HSet(t *testing.T) {

	t.Run("test1", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		db.HSet(nil, nil, nil)

		_, _ = db.HSet([]byte(key), []byte("my_name"), []byte("roseduan"))

		//val := db.HGet([]byte(key), []byte("my_name"))
		//log.Println(string(val))

	})

	t.Run("reopen and set", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()
		//_, _ = db.HSet([]byte(key), []byte("my_hobby"), []byte("coding better"))
		//_, _ = db.HSet([]byte(key), []byte("my_lang"), []byte("Java and Go"))
	})

	//t.Run("multi data", func(t *testing.T) {
	//	db := ReopenDb()
	//	defer db.Close()
	//
	//	rand.Seed(time.Now().Unix())
	//
	//	fieldPrefix := "hash_field_"
	//	valPrefix := "hash_data_"
	//
	//	var res int
	//	for i := 0; i < 100000; i++ {
	//		field := fieldPrefix + strconv.Itoa(rand.Intn(1000000))
	//		val := valPrefix + strconv.Itoa(rand.Intn(1000000))
	//
	//		res, _ = db.HSet([]byte(key), []byte(field), []byte(val))
	//	}
	//	t.Log(res)
	//})
}

func TestFastDB_HGet(t *testing.T) {
	db := ReopenDb()
	defer db.Close()
	//_, _ = db.HSet([]byte(key), []byte("my_name"), []byte("roseduan"))

	val := db.HGet([]byte(key), []byte("my_name"))
	log.Println(string(val))

}
