package operation

import (
	"errors"
	"fmt"
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
	fmt.Printf("delete File %s \n", d.bucket)
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

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(response.Status)
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

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return errors.New(response.Status)
	}
	succeedHostName(host)
	return nil
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
