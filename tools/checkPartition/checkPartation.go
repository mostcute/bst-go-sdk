package main

import (
	"bufio"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	. "io/ioutil"
	"os"
	"path"
	"stroage-go-sdk/operation"
)

const testTmpPath = "./tmpDir"
const testResPath = "./resDir"

var log = logging.Logger("potation check")

func handleText(textfile string, fixPath bool) ([]string, error) {
	var fileList []string
	file, err := os.Open(textfile)
	if err != nil {
		log.Fatalf("Cannot open text file: %s, err: [%v]", textfile, err)
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text() // or
		//line := scanner.Bytes()
		//do_your_function(line)
		if fixPath {
			fileList = append(fileList, fmt.Sprintf("/root/.lotusminer/sealed/s-t01185349-%s", line))
		} else {
			fileList = append(fileList, line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Cannot scanner text file: %s, err: [%v]", textfile, err)
		return nil, err
	}

	return fileList, nil
}

func readAll(path string) []string {
	var all_file []string
	finfo, _ := ReadDir(path)
	for _, x := range finfo {
		real_path := path + "/" + x.Name()
		//fmt.Println(x.Name()," ",x.Size())
		if x.IsDir() {
			fmt.Println(x.Name(), " ", x.Size())
			all_file = append(all_file, readAll(real_path)...)
		} else {
			all_file = append(all_file, real_path)
		}
	}
	return all_file
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		fmt.Println("未发现临时文件夹")
		if os.IsNotExist(err) {
			fmt.Println("临时文件夹不存在")
			err := os.Mkdir(path, os.ModePerm)
			if err != nil {
				fmt.Printf("mkdir failed![%v]\n", err)
				return false, err
			}
			return true, nil
		}
		fmt.Println("stat file error")
		return false, err
	}
	return true, nil
}

func genTempFile(filename string) error {
	fileList := readAll(filename)
	for _, value := range fileList {
		nameList, err := handleText(value, true)
		if err != nil {
			log.Fatal(err)
			return err
		}
		pathExists(testTmpPath)
		filenameWithSuffix := path.Base(value)
		f, err := os.Create(testTmpPath + "/" + filenameWithSuffix) //创建文件
		if err != nil {
			log.Fatal(err)
			return err
		}
		for _, name := range nameList {
			w := bufio.NewWriter(f) //创建新的 Writer 对象
			w.WriteString(name + "\n")
			w.Flush()
		}
		f.Close()
	}
	return nil
}

func checkBstFile(filename string, bstDownload *operation.Downloader) error {
	fileList := readAll(filename)
	for _, value := range fileList {
		nameList, err := handleText(value, false)
		if err != nil {
			log.Fatal(err)
			return err
		}
		pathExists(testResPath)
		filenameWithSuffix := path.Base(value)
		f, err := os.Create(testResPath + "/" + filenameWithSuffix) //创建文件
		if err != nil {
			log.Fatal(err)
			return err
		}
		for _, name := range nameList {
			stats, _ := bstDownload.GetFileExiet(name)
			if stats {
				log.Infof("%s is Exist \n", name)
			} else {
				w := bufio.NewWriter(f) //创建新的 Writer 对象
				w.WriteString(name + "\n")
				w.Flush()
				log.Warnf("%s is Not Found \n", name)
			}
		}
		f.Close()
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

	defer func() {
		log.Info("开始清理临时文件")
		if err := os.RemoveAll(testTmpPath); err != nil {
			log.Errorf("remove all: %s", err)
		}
	}()

	x, err := operation.Load(cf)
	if err != nil {
		log.Error(err)
	}
	bstDownloader := operation.NewDownloader(x)
	filename := ctx.String("file")
	genTempFile(filename)
	checkBstFile(testTmpPath, bstDownloader)
	return nil
}

var proveCmd = &cli.Command{
	Name:  "partation",
	Usage: "partation check",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "file",
			Usage: "potation file dir",
			Value: "./5349_partition",
		},
	},
	Action: runCheck,
}

func main() {
	app := &cli.App{
		Name:    "partation",
		Usage:   "partation check",
		Version: "1.0.0",
		Commands: []*cli.Command{
			proveCmd,
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("%+v", err)
		return
	}
}
