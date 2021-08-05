package fastdb

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
)

func TestFastDB_Set(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := InitDb()
		defer db.Close()

		db.Set(nil, nil)

		err := db.Set([]byte("test_key"), []byte("I am roseduan"))
		if err != nil {
			log.Fatal("write data error ", err)
		}
	})

	t.Run("reopen and set", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		db.Set([]byte("test_key001"), []byte("test_val001"))
		db.Set([]byte("test_key002"), []byte("test_val002"))
	})

	t.Run("large data", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		for i := 0; i < 100; i++ {
			key := "k---" + strconv.Itoa(rand.Intn(100000))
			val := "v---" + strconv.Itoa(rand.Intn(100000))
			err := db.Set([]byte(key), []byte(val))
			if err != nil {
				log.Println("数据写入发生错误 ", err)
			}
		}
	})
}

func TestFastDB_Get(t *testing.T) {

	t.Run("normal situation", func(t *testing.T) {
		db := ReopenDb()
		defer db.Close()

		db.Get(nil)
		db.Get([]byte("hahahaha"))

		val, err := db.Get([]byte("test_key"))
		if err != nil {
			log.Fatal("get val error : ", err)
		}

		t.Log(string(val))

		val, _ = db.Get([]byte("test_key001"))
		t.Log(string(val))

		val, _ = db.Get([]byte("test_key002"))
		t.Log(string(val))
	})

	t.Run("reopen and get", func(t *testing.T) {
		db := ReopenDb()

		val, _ := db.Get([]byte("test_key"))
		log.Println(string(val))
	})

	//t.Run("large data", func(t *testing.T) {
	//	now := time.Now()
	//	db := ReopenDb()
	//	t.Log("reopen db time spent : ", time.Since(now))
	//
	//	defer db.Close()
	//
	//	val, _ := db.Get([]byte("test_key_001"))
	//	t.Log(string(val))
	//
	//	val, _ = db.Get([]byte("test_key_534647"))
	//	t.Log(string(val))
	//
	//	val, _ = db.Get([]byte("test_key_378893"))
	//	t.Log(string(val))
	//})
}
