package fastdb

type FastDb struct {
	config Config // Config info of rosedb.
}

func Open(config Config) (*FastDb, error) {

	return &FastDb{}, nil
}
