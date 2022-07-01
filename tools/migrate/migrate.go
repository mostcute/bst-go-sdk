package main

import (
	"bufio"
	"fmt"
	"git.wutoon.com/lintao/bst-go-sdk/operation"
	"github.com/go-resty/resty/v2"
	logging "github.com/ipfs/go-log/v2"
	qn "github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"strings"
	"sync"
)

type ListReq struct {
	List []List `json:"list"`
}
type List struct {
	ID        int    `json:"id"`
	MFileList string `json:"m_file_list"`
	Idle      bool   `json:"idle"`
}

var mgtClient = resty.New()

var log = logging.Logger("move")

const TaskLoad = 10000

type MigrateFileConf struct {
	FileName string
	Task     chan string
	Data     []string
}

var Tasks = make(chan string, TaskLoad)

func MigrateBst(bstFile string, bstUpload *operation.Uploader, bstDownload *operation.Downloader, bstSRC string, size int64) error {
	if !strings.HasPrefix(bstFile, "/") {
		bstFile = fmt.Sprintf("/%s", bstFile)
	}

	bsdSrcConf, err := operation.Load(bstSRC)
	if err != nil {
		log.Error(err)
	}
	bstSrcDown := operation.NewDownloader(bsdSrcConf)

	stats, err := bstDownload.GetFileExiet(bstFile)
	//if err != nil {
	//	log.Info(err)
	//	return err
	//}
	if stats {
		log.Info("File Exist Skip")
		return nil
	}

	for i := 0; i < 3; i++ {
		var lastbytes io.Reader
		if size > 32 {
			_, lastbytes, err = bstSrcDown.DownloadRangeReader(bstFile, size-32, 32)
			if err != nil {
				log.Info("BST down lastbytes failed")
				return err
			}
		} else {
			_, lastbytes, err = bstSrcDown.DownloadRangeReader(bstFile, 0, size)
			if err != nil {
				log.Info("BST down lastbytes failed")
				return err
			}
		}

		_, reader, err := bstSrcDown.DownloadRangeReader(bstFile, 0, size)
		if err != nil {
			log.Info("BST down src failed")
			return err
		}

		err = bstUpload.UploadFromReader(reader, size, bstFile, true, true, lastbytes)
		if err == nil {
			break
		}
		log.Info("small upload retry", i, err)
	}
	return nil
}

func Migrate(qnFile, bstFile string, qnDownloader *qn.Downloader, bstUpload *operation.Uploader, bstDownload *operation.Downloader, size int64) error {

	if !strings.HasPrefix(bstFile, "/") {
		bstFile = fmt.Sprintf("/%s", bstFile)
	}

	stats, err := bstDownload.GetFileExiet(bstFile)
	//if err != nil {
	//	log.Info(err)
	//	return err
	//}
	if stats {
		log.Info("File Exist Skip")
		return nil
	}
	for i := 0; i < 3; i++ {
		var lastbytes io.Reader
		if size > 32 {
			_, lastbytes, err = qnDownloader.DownloadRangeReader(qnFile, size-32, 32)
			if err != nil {
				log.Info("qiniu down lastbytes failed")
				return err
			}
		} else {
			_, lastbytes, err = qnDownloader.DownloadRangeReader(qnFile, 0, size)
			if err != nil {
				log.Info("qiniu down lastbytes failed")
				return err
			}
		}

		_, reader, err := qnDownloader.DownloadRangeReader(qnFile, 0, size)
		if err != nil {
			log.Info("qiniu down src failed")
			return err
		}

		err = bstUpload.UploadFromReader(reader, size, bstFile, true, true, lastbytes)
		if err == nil {
			break
		}
		log.Info("small upload retry", i, err)
	}
	return nil
}

func (m *MigrateFileConf) readLineTxtV2(url, limit string) ([]string, error) {
	var nameList []string
	url = fmt.Sprintf("http://%s/getlist?limit=%s", url, limit)
	var res ListReq
	resp_, err := mgtClient.R().
		SetResult(&res).
		Get(url)
	if err != nil {
		return nil, err
	}
	if resp_.StatusCode() != 200 {
		log.Fatal(string(resp_.Body()))
		return nil, err
	}
	for _, v := range res.List {
		nameList = append(nameList, v.MFileList)
	}
	return nameList, nil
}

func (m *MigrateFileConf) readLineTxt() ([]string, error) {
	f, err := os.Open(m.FileName)
	var nameList []string
	if err != nil {
		log.Error("Open File Error:", err)
		return nil, err
	}
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			nameList = append(nameList, line)
			//Tasks <- line
		}
		if err != nil {
			if err == io.EOF {
				log.Info("Read File Finish")
				//close(g.Tasks)
				return nameList, nil
			}
			log.Error("Read File Error:", err)
			return nil, err
		}
	}
	return nil, err
}

func (m *MigrateFileConf) readMigreteFile(url, limit string) (err error) {
	m.Data, err = m.readLineTxtV2(url, limit)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (m *MigrateFileConf) readMigreteFileLocal() (err error) {
	m.Data, err = m.readLineTxt()
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func checkSize(qnDownloader *qn.Downloader, name string) int64 {
	size, err := qnDownloader.DownloadCheck(name)
	if err != nil {
		log.Fatal(err)
	}
	return size
}

func runMigrate(ctx *cli.Context) error {
	var cf string
	if os.Getenv("STORE") == "" {
		log.Fatal("Env is Empty")
	} else {
		cf = os.Getenv("STORE")
	}
	if args := ctx.Args(); args.Len() > 0 {
		return fmt.Errorf("invalid command: %q", args.Get(0))
	}
	qnConf, err := qn.Load(ctx.String("qiniu"))
	if err != nil {
		log.Error("load config error")
	}
	qnDownloader := qn.NewDownloader(qnConf)

	//_, err = qnDownloader.DownloadFile("root/.lotus-bench/bench052404401/cache/s-t01000-0/p_aux", "paux")
	//if err != nil {
	//	log.Error(err)
	//}
	x, err := operation.Load(cf)
	if err != nil {
		log.Error(err)
	}
	bstUpload := operation.NewUploader(x)
	bstDownloader := operation.NewDownloader(x)

	var migrater = MigrateFileConf{
		FileName: ctx.String("list"),
		Task:     make(chan string, TaskLoad),
		Data:     make([]string, 0),
	}
	log.Info("start read file ")
	for true {
		migrater.readMigreteFile(ctx.String("url"), ctx.String("limit"))
		if len(migrater.Data) == 0 {
			return nil
		}
		nums := ctx.Uint64("go")
		limit := make(chan struct{}, nums)
		log.Info("nums ", nums)
		var wait sync.WaitGroup
		for _, value := range migrater.Data {
			limit <- struct{}{}
			log.Info("nums ", nums)
			wait.Add(1)
			go func(v string) {
				size := checkSize(qnDownloader, v)
				log.Info(v)
				err = Migrate(v, v, qnDownloader, bstUpload, bstDownloader, size)
				if err != nil {
					log.Error("Migrate failed ", v, " ", err)
				}
				<-limit
				wait.Done()
			}(value)

		}
		wait.Wait()
	}

	return nil
}

func runMigrateBst(ctx *cli.Context) error {
	var cf string
	if os.Getenv("STORE") == "" {
		log.Fatal("Env is Empty")
	} else {
		cf = os.Getenv("STORE")
	}
	if args := ctx.Args(); args.Len() > 0 {
		return fmt.Errorf("invalid command: %q", args.Get(0))
	}

	bstSrc := ctx.String("bstdst")
	bstSrcx, err := operation.Load(bstSrc)
	if err != nil {
		log.Error(err)
	}
	bstSrcDownload := operation.NewDownloader(bstSrcx)

	x, err := operation.Load(cf)
	if err != nil {
		log.Error(err)
	}
	bstUpload := operation.NewUploader(x)
	bstDownloader := operation.NewDownloader(x)

	var migrater = MigrateFileConf{
		FileName: ctx.String("list"),
		Task:     make(chan string, TaskLoad),
		Data:     make([]string, 0),
	}
	log.Info("start read file ")
	for true {
		migrater.readMigreteFile(ctx.String("url"), ctx.String("limit"))
		if len(migrater.Data) == 0 {
			return nil
		}
		nums := ctx.Uint64("go")
		limit := make(chan struct{}, nums)
		log.Info("nums ", nums)
		var wait sync.WaitGroup
		for _, value := range migrater.Data {
			limit <- struct{}{}
			log.Info("nums ", nums)
			wait.Add(1)
			go func(v string) {
				size, err := bstSrcDownload.GetFileSize(v)
				if err != nil {
					log.Error("Get Size failed ", v, " ", err)
				} else {
					log.Info(v)
					err = MigrateBst(v, bstUpload, bstDownloader, bstSrc, size)
					if err != nil {
						log.Error("Migrate failed ", v, " ", err)
					}
				}
				<-limit
				wait.Done()
			}(value)

		}
		wait.Wait()
	}

	return nil
}

func runListGet(ctx *cli.Context) error {
	if args := ctx.Args(); args.Len() > 0 {
		return fmt.Errorf("invalid command: %q", args.Get(0))
	}
	f, err := os.Create(ctx.String("file"))
	if err != nil {
		fmt.Println(err)
		f.Close()
		return err
	}

	defer func() {
		err = f.Close()
		fmt.Println("file written successfully")
		if err != nil {
			fmt.Println(err)
			return
		}
	}()

	qnConf, err := qn.Load(ctx.String("qiniu"))
	if err != nil {
		log.Error("load config error")
	}

	lister := qn.NewLister(qnConf)
	list := lister.ListPrefix("")
	//print entries
	for _, entry := range list {
		fmt.Fprintln(f, entry)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

func runCheck(ctx *cli.Context) error {
	var cf string
	if os.Getenv("STORE") == "" {
		log.Fatal("Env is Empty")
	} else {
		cf = os.Getenv("STORE")
	}
	if args := ctx.Args(); args.Len() > 0 {
		return fmt.Errorf("invalid command: %q", args.Get(0))
	}
	x, err := operation.Load(cf)
	if err != nil {
		log.Error(err)
	}
	bstDownloader := operation.NewDownloader(x)
	var migrater = MigrateFileConf{
		FileName: ctx.String("list"),
		Task:     make(chan string, TaskLoad),
		Data:     make([]string, 0),
	}
	err = migrater.readMigreteFileLocal()
	if err != nil {
		log.Error(err)
		return err
	}
	nums := ctx.Uint64("go")
	limit := make(chan struct{}, nums)
	log.Info("nums ", nums)
	var wait sync.WaitGroup
	for _, value := range migrater.Data {
		limit <- struct{}{}
		log.Info("nums ", nums)
		wait.Add(1)
		go func(v string) {
			stats, _ := bstDownloader.GetFileExiet(v)
			if !stats {
				fmt.Println(v)
			}
			<-limit
			wait.Done()
		}(value)
	}
	return nil
}

var listCmd = &cli.Command{
	Name:  "list",
	Usage: "list qn bucket",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "qiniu",
			Usage: "qiniu Env",
			Value: "./cfg_qiniu.toml",
		},
		&cli.StringFlag{
			Name:  "file",
			Usage: "list file path",
			Value: "./qiniu_list.txt",
		},
	},
	Action: runListGet,
}

var proveCmd = &cli.Command{
	Name:  "move",
	Usage: "move qn to bst",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "qiniu",
			Usage: "qiniu Env",
			Value: "./cfg_qiniu.toml",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "migrate List",
			Value: "127.0.0.1:11345",
		},
		&cli.StringFlag{
			Name:  "limit",
			Usage: "control data limit",
			Value: "1000",
		},
		&cli.Uint64Flag{
			Name:  "go",
			Usage: "limit file numbers to upload",
			Value: 1,
		},
		&cli.BoolFlag{
			Name:  "bytes",
			Usage: "use bytes mode",
			Value: false,
		},
	},
	Action: runMigrate,
}

var migrateBst = &cli.Command{
	Name:  "bstmove",
	Usage: "move bst to bst",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "bstdst",
			Usage: "dst bst Env",
			Value: "./cfg_bst_dst.toml",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "migrate List",
			Value: "127.0.0.1:11345",
		},
		&cli.StringFlag{
			Name:  "limit",
			Usage: "control data limit",
			Value: "1000",
		},
		&cli.Uint64Flag{
			Name:  "go",
			Usage: "limit file numbers to upload",
			Value: 1,
		},
		&cli.BoolFlag{
			Name:  "bytes",
			Usage: "use bytes mode",
			Value: false,
		},
	},
	Action: runMigrateBst,
}

var checkCmd = &cli.Command{
	Name:  "check",
	Usage: "move bst file",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "go",
			Usage: "limit file numbers to upload",
			Value: 1,
		},
		&cli.StringFlag{
			Name:  "list",
			Usage: "list file path",
			Value: "./list",
		},
	},
	Action: runCheck,
}

func main() {
	app := &cli.App{
		Name:    "move",
		Usage:   "move file from qn to bst",
		Version: "1.0.0",
		Commands: []*cli.Command{
			proveCmd,
			listCmd,
			migrateBst,
			checkCmd,
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("%+v", err)
		return
	}
}
