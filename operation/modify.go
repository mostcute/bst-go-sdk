package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qiniupd/qiniu-go-sdk/x/log.v7"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

type Modify struct {
	bucket  string
	ioHosts []string
	queryer *Queryer
}

type ExHeader struct {
	Floder []string `json:"floder"`
}

type MetaInfo struct {
	Name      string   `json:"name"`
	Size      int64    `json:"size"`
	Type      int      `json:"type"`
	Time      int64    `json:"time"`
	Url       string   `json:"url"`
	Dir       bool     `json:"isDir"`
	Exheaders ExHeader `json:"extern-headers"`
}

type BstFiles struct {
	Data []BstFile `json:"Data"`
	Len  int       `json:"Len"`
}

type BstFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Type int    `json:"type"`
	Time int64  `json:"time"`
	Url  string `json:"url"`
	Dir  bool   `json:"isDir"`
}

var modifyClient = &http.Client{
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

func (d *Modify) nextHost() string {
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

func (d *Modify) deleteFileInner(key string) error {
	host := d.nextHost()
	//fmt.Printf("delete File %s \n", d.bucket)
	url := fmt.Sprintf("http://%s/objects/deletefile/%s/%s", host, d.bucket, key)
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

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(string(body))
	}
	succeedHostName(host)
	return nil
}

func (d *Modify) renameInner(key string, newName string) error {
	host := d.nextHost()
	fmt.Printf("rename File %s \n", d.bucket)
	url := fmt.Sprintf("http://%s/objects/rename/%s/%s", host, d.bucket, key)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		failHostName(host)
		return err
	}
	req.Header.Set("Accept-Encoding", "")
	req.Header.Set("newname", newName)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(string(body))
	}
	succeedHostName(host)
	return nil
}

func (d *Modify) metaInfoInner(key string) (*MetaInfo, error) {
	host := d.nextHost()
	log.Infof("metaInfo File %s \n", d.bucket)
	url := fmt.Sprintf("http://%s/objects/metadetail", host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("object", key)
	req.Header.Set("bucket", d.bucket)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return nil, errors.New(string(body))
	}

	metaInfoJson := MetaInfo{}
	jsonErr := json.Unmarshal(body, &metaInfoJson)

	if jsonErr != nil {
		return nil, jsonErr
	}
	if metaInfoJson.Exheaders.Floder != nil {
		metaInfoJson.Dir = true
	}
	succeedHostName(host)
	return &metaInfoJson, nil
}

func (d *Modify) listObjInner(prefix string, size int) (*BstFiles, error) {
	host := d.nextHost()
	log.Infof("listObject Files %s \n", d.bucket)
	url := fmt.Sprintf("http://%s/objects/listobject/%s", host, d.bucket)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("size", fmt.Sprintf("%d", size))
	req.Header.Set("Prefix", prefix)
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return nil, errors.New(string(body))
	}
	bstFiles := BstFiles{}
	jsonErr := json.Unmarshal(body, &bstFiles)

	if jsonErr != nil {
		return nil, jsonErr
	}
	succeedHostName(host)
	return &bstFiles, nil
}

func NewModifier(c *Config) *Modify {

	var queryer *Queryer = nil

	deleter := Modify{
		bucket:  c.Bucket,
		ioHosts: dupStrings(c.IoHosts),
		queryer: queryer,
	}
	shuffleHosts(deleter.ioHosts)
	return &deleter
}

func (d *Modify) DeleteFile(key string) (err error) {
	for i := 0; i < 3; i++ {
		err = d.deleteFileInner(key)
		if err == nil {
			return nil
		}
	}
	return err
}

func (d *Modify) RenameFile(key, newname string) (err error) {
	for i := 0; i < 3; i++ {
		err = d.renameInner(key, newname)
		if err == nil {
			return nil
		}
	}
	return err
}

func (d *Modify) MetaInfo(key string) (metaInfo *MetaInfo, err error) {
	for i := 0; i < 3; i++ {
		metaInfo, err = d.metaInfoInner(key)
		if err == nil {
			break
		}
	}
	return
}

func (d *Modify) ListObject(prefix string, size int) (bstFiles *BstFiles, err error) {
	for i := 0; i < 3; i++ {
		bstFiles, err = d.listObjInner(prefix, size)
		if err == nil {
			break
		}
	}
	return
}

func (d *Modify) LinkGen(name string, protocol string) string {
	host := d.nextHost()
	return fmt.Sprintf("%s://%s/objects/getfile/%s/%s", protocol, host, d.bucket, name)
}
