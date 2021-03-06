package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
)

var downloadClient = &http.Client{
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

type Downloader struct {
	bucket  string
	ioHosts []string
	queryer *Queryer
}

type wrapper struct {
	s    io.ReadCloser
	host string
}

type ExternHeaders struct {
	Accept      []string `json:"Accept"`
	Connection  []string `json:"Connection"`
	ContentType []string `json:"Content-Type"`
	Floder      []string `json:"Floder"`
}

type Res struct {
	Size    int64         `json:"size"`
	Eheader ExternHeaders `json:"extern-headers"`
}

func (w *wrapper) Read(p []byte) (n int, err error) {
	n, err = w.s.Read(p)
	if err != nil && err != io.EOF {
		elog.Info("read interrupt", w.host, err)
	}
	return
}

func (w *wrapper) Close() error {
	return w.s.Close()
}

func NewDownloader(c *Config) *Downloader {

	var queryer *Queryer = nil

	downloader := Downloader{
		bucket:  c.Bucket,
		ioHosts: dupStrings(c.IoHosts),
		queryer: queryer,
	}
	shuffleHosts(downloader.ioHosts)
	return &downloader
}
func NewDownloaderV2() *Downloader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewDownloader(c)
}

func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path)
		if err == nil {
			return
		}
	}
	return
}

func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key)
		if err == nil {
			break
		}
	}
	return
}

func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	for i := 0; i < 3; i++ {
		l, data, err = d.downloadRangeBytesInner(key, offset, size)
		if err == nil {
			break
		}
	}
	return
}

func (d *Downloader) DownloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		l, reader, err = d.downloadRangeReaderInner(key, offset, size, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *Downloader) DownloadRaw(key string, headers http.Header) (resp *http.Response, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		resp, _, err = d.downloadRawInner(key, headers, failedIoHosts)
		if err == nil {
			return
		}
	}
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

var curIoHostIndex uint32 = 0

func (d *Downloader) nextHost() string {
	ioHosts := d.ioHosts
	if d.queryer != nil {
		if hosts := d.queryer.QueryIoHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			ioHosts = hosts
		}
	}
	switch len(ioHosts) {
	case 0:
		panic("No Io hosts is configured")
	case 1:
		return ioHosts[0]
	default:
		var ioHost string
		for i := 0; i <= len(ioHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curIoHostIndex, 1) - 1)
			ioHost = ioHosts[index%len(ioHosts)]
			if isHostNameValid(ioHost) {
				break
			}
		}
		return ioHost
	}
}

func (d *Downloader) downloadFileInner(key, path string) (*os.File, error) {
	//if strings.HasPrefix(key, "/") {
	//	key = strings.TrimPrefix(key, "/")
	//}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	host := d.nextHost()

	fmt.Println("remote path", key)
	url := fmt.Sprintf("http://%s/objects/getfile/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	req.Header.Set("User-Agent", rpc.UserAgent)
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		succeedHostName(host)
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *Downloader) downloadBytesInner(key string) ([]byte, error) {
	//if strings.HasPrefix(key, "/") {
	//	key = strings.TrimPrefix(key, "/")
	//}
	host := d.nextHost()

	url := fmt.Sprintf("http://%s/objects/getfile/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	return ioutil.ReadAll(response.Body)
}

func (d *Downloader) downloadRangeReaderInner(key string, offset, size int64, failedIoHosts map[string]struct{}) (int64, io.ReadCloser, error) {
	headers := make(http.Header)
	headers.Set("Range", generateRange(offset, size))
	host := d.nextHost()

	url := fmt.Sprintf("%s/objects/getfile/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	req.Header.Set("Range", generateRange(offset, size))
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, err
	}
	w := wrapper{
		s:    response.Body,
		host: host,
	}
	return l, &w, err
}

func (d *Downloader) downloadRawInner(key string, headers http.Header, failedIoHosts map[string]struct{}) (*http.Response, string, error) {
	host := d.nextHost()

	url := fmt.Sprintf("%s/objects/getfile/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, host, err
	}
	for headerName, headerValue := range headers {
		req.Header[headerName] = headerValue
	}
	response, err := downloadClient.Do(req)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, host, err
	}
	return response, host, nil
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size)
}

func (d *Downloader) downloadRangeBytesInner(key string, offset, size int64) (int64, []byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	host := d.nextHost()

	url := fmt.Sprintf("%s/objects/getfile/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}

	req.Header.Set("Range", generateRange(offset, size))
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		failHostName(host)
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		failHostName(host)
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		failHostName(host)
	} else {
		succeedHostName(host)
	}
	return l, b, err
}

func getTotalLength(crange string) (int64, error) {
	cr := strings.Split(crange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + crange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}

func (d *Downloader) getFileExietInner(fileName string) (string, error) {
	host := d.nextHost()
	//elog.Infof("Get File Exiet %s \n", d.bucket)
	url := fmt.Sprintf("http://%s/objects/getfile/%s/%s", host, d.bucket, fileName)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		failHostName(host)
		return err.Error(), err
	}
	req.Header.Set("Accept-Encoding", "")
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return err.Error(), err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		failHostName(host)
		return err.Error(), errors.New(response.Status)
	}
	succeedHostName(host)
	return response.Status, nil
}

func (d *Downloader) getFileSizeInner(fileName string) (int64, error) {
	host := d.nextHost()
	url := fmt.Sprintf("http://%s/objects/metadetail", host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return -1, err
	}
	req.Header.Set("object", fileName)
	req.Header.Set("bucket", d.bucket)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return -1, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		failHostName(host)
		return -1, errors.New(response.Status)
	}

	b, err := ioutil.ReadAll(response.Body)
	res := &Res{}
	err = json.Unmarshal(b, res)
	if err != nil {
		fmt.Println(err)
	}
	succeedHostName(host)
	return res.Size, nil

}

func (d *Downloader) GetFileExiet(fileName string) (bool, error) {
	var err error
	for i := 0; i < 3; i++ {
		res, err := d.getFileExietInner(fileName)
		if err == nil {
			if find := strings.Contains(res, "200 OK"); find {
				return true, nil
			} else {
				return false, errors.New(res)
			}
		}
	}
	return false, err
}

func (d *Downloader) GetFileSize(fileName string) (int64, error) {
	var err error
	for i := 0; i < 3; i++ {
		res, err := d.getFileSizeInner(fileName)
		if err == nil {
			return res, nil
		}
	}
	return -1, err
}
