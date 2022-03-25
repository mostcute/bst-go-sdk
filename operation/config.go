package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type Config struct {
	IoHosts       []string `json:"io_hosts" toml:"io_hosts"`
	Bucket        string   `json:"bucket" toml:"bucket"`
	PartSize      int64    `json:"part" toml:"part"`
	Retry         int      `json:"retry" toml:"retry"`
	BaseTimeoutMs int64    `json:"base_timeout_ms" toml:"base_timeout_ms"`
}

func dupStrings(s []string) []string {
	if s == nil || len(s) == 0 {
		return s
	}
	to := make([]string, len(s))
	copy(to, s)
	return to
}

func Load(file string) (*Config, error) {
	var configuration Config
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	ext := path.Ext(file)
	ext = strings.ToLower(ext)
	if ext == ".json" {
		err = json.Unmarshal(raw, &configuration)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &configuration)
	} else {
		return nil, errors.New("configuration format invalid!")
	}

	return &configuration, err
}

var g_conf *Config

var confLock sync.Mutex

func getConf() *Config {
	up := os.Getenv("STORE")
	if up == "" {
		elog.Warn("not set store environment")
		return nil
	}
	confLock.Lock()
	defer confLock.Unlock()
	if g_conf != nil {
		return g_conf
	}
	c, err := Load(up)
	if err != nil {
		elog.Warn("load conf failed", up, err)
		return nil
	}
	g_conf = c
	watchConfig(up)
	return c
}

func watchConfig(filename string) {
	initWG := sync.WaitGroup{}
	initWG.Add(1)
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			elog.Fatal(err)
		}
		defer watcher.Close()

		configFile := filepath.Clean(filename)
		configDir, _ := filepath.Split(configFile)
		realConfigFile, _ := filepath.EvalSymlinks(filename)

		eventsWG := sync.WaitGroup{}
		eventsWG.Add(1)
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok { // 'Events' channel is closed
						eventsWG.Done()
						return
					}
					currentConfigFile, _ := filepath.EvalSymlinks(filename)
					// we only care about the config file with the following cases:
					// 1 - if the config file was modified or created
					// 2 - if the real path to the config file changed (eg: k8s ConfigMap replacement)
					const writeOrCreateMask = fsnotify.Write | fsnotify.Create
					if (filepath.Clean(event.Name) == configFile &&
						event.Op&writeOrCreateMask != 0) ||
						(currentConfigFile != "" && currentConfigFile != realConfigFile) {
						realConfigFile = currentConfigFile
						c, err := Load(realConfigFile)
						fmt.Printf("re reading config file: error %v\n", err)
						if err == nil {
							g_conf = c
						}
					} else if filepath.Clean(event.Name) == configFile &&
						event.Op&fsnotify.Remove&fsnotify.Remove != 0 {
						eventsWG.Done()
						return
					}

				case err, ok := <-watcher.Errors:
					if ok { // 'Errors' channel is not closed
						fmt.Printf("watcher error: %v\n", err)
					}
					eventsWG.Done()
					return
				}
			}
		}()
		watcher.Add(configDir)
		initWG.Done()   // done initializing the watch in this go routine, so the parent routine can move on...
		eventsWG.Wait() // now, wait for event loop to end in this go-routine...
	}()
	initWG.Wait() // make sure that the go routine above fully ended before returning
}
