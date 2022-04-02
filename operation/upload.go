package operation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var curUpHostIndex uint32 = 0

type Uploader struct {
	bucket        string
	upHosts       []string
	partSize      int64
	upConcurrency int
	overview      bool
	queryer       *Queryer
}

var uploadClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 10 * time.Minute,
}

func (p *Uploader) Upload(file string, key string, overView bool) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	//key = strings.TrimPrefix(key, "/")
	f, err := os.Open(file)
	if err != nil {
		elog.Info("open file failed: ", file, err)
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		elog.Info("get file stat failed: ", err)
		return err
	}

	for i := 0; i < 3; i++ {
		err = p.put2(context.Background(), nil, key, newReaderAtNopCloser(f), fInfo.Size(), p.bucket, p.partSize, overView)
		if err == nil {
			break
		}
		elog.Info("small upload retry", i, err)
	}
	return
}

func (p *Uploader) UploadFromReader(reader io.Reader, size int64, key string, overView bool) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()
	//key = strings.TrimPrefix(key, "/")

	for i := 0; i < 3; i++ {
		err = p.put(context.Background(), nil, key, reader, size, p.bucket, p.partSize, overView)
		if err == nil {
			break
		}
		elog.Info("small upload retry", i, err)
	}
	return
}

func (p *Uploader) UploadBytes(data []byte, key string, overView bool) (err error) {
	t := time.Now()
	defer func() {
		elog.Info("up time ", key, time.Now().Sub(t))
	}()

	for i := 0; i < 3; i++ {
		err = p.put(context.Background(), nil, key, bytes.NewReader(data), int64(len(data)), p.bucket, p.partSize, overView)
		if err == nil {
			break
		}
		elog.Info("small upload retry", i, err)
	}
	return
}

func NewUploader(c *Config) *Uploader {
	var queryer *Queryer = nil

	if len(c.IoHosts) > 0 {
		queryer = NewQueryer(c)
	}

	return &Uploader{
		bucket:   c.Bucket,
		upHosts:  dupStrings(c.IoHosts),
		partSize: c.PartSize,
		queryer:  queryer,
	}
}

func (p Uploader) chooseUpHost() string {
	switch len(p.upHosts) {
	case 0:
		panic("No Up hosts is configured")
	case 1:
		return p.upHosts[0]
	default:
		var upHost string
		for i := 0; i <= len(p.upHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curUpHostIndex, 1) - 1)
			upHost = p.upHosts[index%len(p.upHosts)]
			if isHostNameValid(upHost) {
				break
			}
		}
		return upHost
	}
}

func (p Uploader) put(ctx context.Context, ret interface{}, key string, data io.Reader, size int64, bucket string,
	blockSize int64, overView bool) error {

	upHost := p.chooseUpHost()
	url := "http://" + upHost + "/objects/put/" + bucket

	if key != "" {
		url += "/" + key
	}
	elog.Debug("Put2", url)
	req, err := http.NewRequest("PUT", url, data)
	if err != nil {
		failHostName(upHost)
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("blocksize", strconv.FormatInt(blockSize, 10))
	req.Header.Set("overwrite", strconv.FormatBool(overView))

	req.ContentLength = size
	resp, err := uploadClient.Do(req)
	if err != nil {
		failHostName(upHost)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		failHostName(upHost)
		return errors.New(resp.Status)
	}
	succeedHostName(upHost)
	return nil
}

func (p Uploader) put2(ctx context.Context, ret interface{}, key string, data io.ReaderAt, size int64, bucket string,
	blockSize int64, overView bool) error {

	upHost := p.chooseUpHost()
	url := "http://" + upHost + "/objects/put/" + bucket

	if key != "" {
		url += "/" + key
	}
	elog.Debug("Put2", url)
	req, err := http.NewRequest("PUT", url, io.NewSectionReader(data, 0, size))
	if err != nil {
		failHostName(upHost)
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("blocksize", strconv.FormatInt(blockSize, 10))
	req.Header.Set("overwrite", strconv.FormatBool(overView))

	req.ContentLength = size
	resp, err := uploadClient.Do(req)
	if err != nil {
		failHostName(upHost)
		return err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		failHostName(upHost)
		return errors.New(string(body))
	}
	succeedHostName(upHost)
	return nil
}

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtNopCloser struct {
	io.ReaderAt
}

func (readerAtNopCloser) Close() error { return nil }

// newReaderAtNopCloser returns a readerAtCloser with a no-op Close method wrapping
// the provided ReaderAt r.
func newReaderAtNopCloser(r io.ReaderAt) readerAtCloser {
	return readerAtNopCloser{r}
}
