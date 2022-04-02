package main

import (
	"bufio"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	qn "github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"strings"
	"stroage-go-sdk/operation"
)

var log = logging.Logger("move")

const TaskLoad = 10000

type MigrateFileConf struct {
	FileName string
	Task     chan string
	Data     []string
}

var Tasks = make(chan string, TaskLoad)

func Migrate(qnFile, bstFile string, qnDownloader *qn.Downloader, bstUpload *operation.Uploader) error {
	srcFileByte, err := qnDownloader.DownloadBytes(qnFile)
	if err != nil {
		log.Info("qiniu down src failed")
		return err
	}
	if !strings.HasPrefix(bstFile, "/") {
		bstFile = fmt.Sprintf("/%s", bstFile)
	}
	err = bstUpload.UploadBytes(srcFileByte, bstFile, true)
	if err != nil {
		log.Info("BST upload dst failed")
		return err
	}
	return nil
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
			Tasks <- line
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

func (m *MigrateFileConf) readMigreteFile() (err error) {
	m.Data, err = m.readLineTxt()
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
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

	var migrater = MigrateFileConf{
		FileName: ctx.String("list"),
		Task:     make(chan string, TaskLoad),
		Data:     make([]string, 0),
	}
	migrater.readMigreteFile()

	for _, value := range migrater.Data {
		log.Info(value)
		err = Migrate(value, value, qnDownloader, bstUpload)
		if err != nil {
			log.Error("Migrate failed ", value, " ", err)
			continue
		}
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
			Name:  "list",
			Usage: "migrate List",
			Value: "./qiniu_list.txt",
		},
	},
	Action: runMigrate,
}

func main() {
	app := &cli.App{
		Name:    "move",
		Usage:   "move file from qn to bst",
		Version: "1.0.0",
		Commands: []*cli.Command{
			proveCmd,
			listCmd,
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("%+v", err)
		return
	}
}
