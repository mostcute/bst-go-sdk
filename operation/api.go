package operation

import "os"

type ConfigInterface interface {
	Load(file string) (*Config, error)
	//DownloadFile(key string, path string) (f *os.File, err error)
}

type UploadInterface interface {
	Upload(file string, key string, overview bool) error
}

type DownloadInterface interface {
	DownloadFile(key string, path string) (f *os.File, err error)
}

type SDK struct {
	ConfigInterface
	UploadInterface
}
