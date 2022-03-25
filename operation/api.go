package operation

type Interface interface {
	Load(file string) (*Config, error)
	//Upload(file string, key string, overview bool) error
	//DownloadFile(key string, path string) (f *os.File, err error)
}

type SDK struct {
	Interface
}
