package fastdb

const (
	defaultAddr = "127.0.0.1:6378"
)

type Config struct {
	Addr string `json:"host" toml:"host"` // server host

}

// DefaultConfig get the default config.
func DefaultConfig() Config {
	return Config{
		Addr: defaultAddr,
	}
}
