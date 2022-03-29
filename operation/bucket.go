package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

type Bucketer struct {
	bucket  string
	ioHosts []string
	queryer *Queryer
}

type ListBucketReq []struct {
	Name      string `json:"Name"`
	SizeLimit int    `json:"SizeLimit"`
	Time      int    `json:"Time"`
}

type ListObjectReq []struct {
	Bucket  string `json:"bucket"`
	Name    string `json:"name"`
	Version int    `json:"version"`
	Size    int    `json:"size"`
	Time    int    `json:"time"`
}

var bucketClient = &http.Client{
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

func (b *Bucketer) nextBucketHost() string {
	ioHosts := b.ioHosts
	if b.queryer != nil {
		if hosts := b.queryer.QueryIoHosts(false); len(hosts) > 0 {
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

func (b *Bucketer) makeBucketInner(bucketName string) error {
	host := b.nextBucketHost()
	fmt.Printf("make Bucket %s", b.bucket)
	url := fmt.Sprintf("http://%s/objects/makebucket/%s", host, bucketName)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		failHostName(host)
		return err
	}
	req.Header.Set("Accept-Encoding", "")
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(response.Status)
	}
	succeedHostName(host)
	return nil
}

func (b *Bucketer) deleteBucketInner(bucketName string) error {
	host := b.nextBucketHost()
	fmt.Printf("delete Bucket %s \n", b.bucket)
	url := fmt.Sprintf("http://%s/objects/deletebucket/%s", host, bucketName)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		failHostName(host)
		return err
	}
	req.Header.Set("Accept-Encoding", "")
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(response.Status)
	}
	succeedHostName(host)
	return nil
}

func (b *Bucketer) listBucketInner() (ListBucketReq, error) {
	host := b.nextBucketHost()
	fmt.Printf("list Bucket %s \n", b.bucket)
	url := fmt.Sprintf("http://%s/objects/listbucket", host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
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

	body, readErr := ioutil.ReadAll(response.Body)
	if readErr != nil {
		return nil, readErr
	}

	listReq := ListBucketReq{}
	jsonErr := json.Unmarshal(body, &listReq)
	if jsonErr != nil {
		return nil, jsonErr
	}
	succeedHostName(host)
	return listReq, nil
}

func (b *Bucketer) getBucketInfoInner(bucketName string) (string, error) {
	host := b.nextBucketHost()
	elog.Infof("get Bucket info %s \n", b.bucket)
	url := fmt.Sprintf("http://%s/objects/getbucket/%s", host, bucketName)
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

func (b *Bucketer) listObjectInfoInner(bucketName string) (ListObjectReq, error) {
	host := b.nextBucketHost()
	elog.Infof("list Bucket Object %s \n", b.bucket)
	url := fmt.Sprintf("http://%s/objects/listobject/%s", host, bucketName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
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

	body, readErr := ioutil.ReadAll(response.Body)
	if readErr != nil {
		return nil, readErr
	}

	listReq := ListObjectReq{}
	jsonErr := json.Unmarshal(body, &listReq)
	if jsonErr != nil {
		return nil, jsonErr
	}
	succeedHostName(host)
	return listReq, nil
}

func NewBucketer(c *Config) *Bucketer {

	var queryer *Queryer = nil

	bucketer := Bucketer{
		bucket:  c.Bucket,
		ioHosts: dupStrings(c.IoHosts),
		queryer: queryer,
	}
	shuffleHosts(bucketer.ioHosts)
	return &bucketer
}

func (b *Bucketer) MakeBucket(bucketName string) (err error) {
	for i := 0; i < 3; i++ {
		err = b.makeBucketInner(bucketName)
		if err == nil {
			return nil
		}
	}
	return err
}

func (b *Bucketer) DeleteBucket(bucketName string) (err error) {
	for i := 0; i < 3; i++ {
		err = b.deleteBucketInner(bucketName)
		if err == nil {
			return nil
		}
	}
	return err
}

func (b *Bucketer) ListBucket() (ListBucketReq, error) {
	var err error
	for i := 0; i < 3; i++ {
		list, err := b.listBucketInner()
		if err == nil {
			return list, nil
		}
	}
	return nil, err
}

func (b *Bucketer) GetBucketInfo(bucketName string) (string, error) {
	var err error
	for i := 0; i < 3; i++ {
		res, err := b.getBucketInfoInner(bucketName)
		if err == nil {
			return res, nil
		}
	}
	return err.Error(), err
}

func (b *Bucketer) ListObject(bucketName string) (ListObjectReq, error) {
	var err error
	for i := 0; i < 3; i++ {
		list, err := b.listObjectInfoInner(bucketName)
		if err == nil {
			return list, nil
		}
	}
	return nil, err
}
