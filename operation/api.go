package operation

import "os"

type ConfigInterface interface {
	Load(file string) (*Config, error)
}

type UploadInterface interface {
	Upload(file string, key string, overview bool) error
	UploadBytes(data []byte, key string, overView bool) (err error)
}

type DownloadInterface interface {
	DownloadFile(key string, path string) (f *os.File, err error)
	DownloadBytes(key string) (data []byte, err error)
	DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error)
	GetFileExiet(fileName string) (bool, error)
}

type ModifyInterface interface {
	DeleteFile(key string) (err error)
	RenameFile(key, newname string) (err error)
}

type BucketInterface interface {
	MakeBucket(bucketName string) (err error)
	DeleteBucket(bucketName string) (err error)
	ListBucket() (ListBucketReq, error)
	GetBucketInfo(bucketName string) (string, error)
	ListObject(bucketName, prefix, size, page string) (ListObjectReq, error)
}
