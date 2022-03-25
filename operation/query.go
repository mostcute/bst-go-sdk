package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kirsle/configdir"
)

var queryClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 1 * time.Second,
}

var (
	cacheMap         sync.Map
	cacheUpdaterLock sync.Mutex
	cachePersisting  uint32 = 0
	cacheDirectory          = configdir.LocalCache("qiniu", "go-sdk")
)

type (
	Queryer struct {
		ak      string
		bucket  string
		ucHosts []string
	}

	cache struct {
		CachedHosts    cachedHosts `json:"hosts"`
		CacheExpiredAt time.Time   `json:"expired_at"`
	}

	cachedHosts struct {
		Hosts []cachedHost `json:"hosts"`
	}

	cachedHost struct {
		Ttl int64                `json:"ttl"`
		Io  cachedServiceDomains `json:"io"`
	}

	cachedServiceDomains struct {
		Domains []string `json:"domains"`
	}
)

func init() {
	loadQueryersCache()
}

func NewQueryer(c *Config) *Queryer {
	queryer := Queryer{
		ucHosts: c.IoHosts,
		bucket:  c.Bucket,
	}
	shuffleHosts(queryer.ucHosts)
	return &queryer
}

func (queryer *Queryer) QueryIoHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Io.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) fromDomainsToUrls(https bool, domains []string) (urls []string) {
	urls = make([]string, len(domains))
	for i, domain := range domains {
		if strings.Contains(domain, "://") {
			urls[i] = domain
		} else if https {
			urls[i] = fmt.Sprintf("https://%s", domain)
		} else {
			urls[i] = fmt.Sprintf("http://%s", domain)
		}
	}
	return urls
}

func (queryer *Queryer) query() (*cache, error) {
	var err error
	c := queryer.getCache()
	if c == nil {
		return func() (*cache, error) {
			var err error
			cacheUpdaterLock.Lock()
			defer cacheUpdaterLock.Unlock()
			c := queryer.getCache()
			if c == nil {
				if c, err = queryer.mustQuery(); err != nil {
					return nil, err
				} else {
					queryer.setCache(c)
					saveQueryersCache()
					return c, nil
				}
			} else {
				return c, nil
			}
		}()
	} else {
		if c.CacheExpiredAt.Before(time.Now()) {
			queryer.asyncRefresh()
		}
		return c, err
	}
}

func (queryer *Queryer) mustQuery() (c *cache, err error) {

	return
}

func (queryer *Queryer) asyncRefresh() {
	go func() {
		var err error

		cacheUpdaterLock.Lock()
		defer cacheUpdaterLock.Unlock()

		c := queryer.getCache()
		if c == nil || c.CacheExpiredAt.Before(time.Now()) {
			if c, err = queryer.mustQuery(); err == nil {
				queryer.setCache(c)
				saveQueryersCache()
			}
		}
	}()
}

func (queryer *Queryer) getCache() *cache {
	value, ok := cacheMap.Load(queryer.cacheKey())
	if !ok {
		return nil
	}
	return value.(*cache)
}

func (queryer *Queryer) setCache(c *cache) {
	cacheMap.Store(queryer.cacheKey(), c)
}

func (queryer *Queryer) cacheKey() string {
	return fmt.Sprintf("%s:%s", queryer.bucket, queryer.ak)
}

var curUcHostIndex uint32 = 0

func loadQueryersCache() error {
	cacheFile, err := os.Open(filepath.Join(cacheDirectory, "query-cache.json"))

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	err = json.NewDecoder(cacheFile).Decode(&m)
	if err != nil {
		return err
	}

	for key, value := range m {
		cacheMap.Store(key, value)
	}
	return nil
}

func saveQueryersCache() error {
	cacheDirInfo, err := os.Stat(cacheDirectory)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(cacheDirectory, 0700); err != nil {
				return err
			}
		} else {
			return err
		}
	} else if !cacheDirInfo.IsDir() {
		return errors.New("cache directory path is occupied and not directory")
	}

	if !atomic.CompareAndSwapUint32(&cachePersisting, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&cachePersisting, 1)

	cacheFile, err := os.OpenFile(filepath.Join(cacheDirectory, "query-cache.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	cacheMap.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(*cache)
		return true
	})

	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = cacheFile.Write(bytes)
	return err
}
