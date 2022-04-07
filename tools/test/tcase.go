package main

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"git.wutoon.com/lintao/bst-go-sdk/operation"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
)

var log = logging.Logger("Test Case")

const testBucketName = "testCase"
const testTmpPath = "./tmpDir"
const verifyTmpPath = "./verifyDir"
const fileChunk = 8192 // we settle for 8KB 8192

var FileSize = []float64{1024, 10240, 102400, 1048576, 10485760, 10485760, 1073741824, 2147483648}

type TestCase struct {
	Config     *operation.Config
	Uploader   *operation.Uploader
	Downloader *operation.Downloader
	Bucketer   *operation.Bucketer
	Modify     *operation.Modify
	SrcMd5     []string
}

//生成随机字符
func createRandomString(len int) string {
	var container string
	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	b := bytes.NewBufferString(str)
	length := b.Len()
	bigInt := big.NewInt(int64(length))
	for i := 0; i < len; i++ {
		randomInt, _ := rand.Int(rand.Reader, bigInt)
		container += string(str[randomInt.Int64()])
	}
	return container
}

// 创建指定大小文件
func CreateFixedFile(size float64, fileName string) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		log.Errorf("Create File Err %s", err)
	}
	defer file.Close()
	source := createRandomString(10)
	count := math.Ceil(size / float64(len(source)))
	_, err = file.WriteString(strings.Repeat(source, int(count)))
	HandleErr("Write File ", err)
}

// 错误处理
func HandleErr(title string, err error) {
	if err != nil {
		log.Fatal(title, " ", err)
	} else {
		log.Infof("%s Success", title)
	}
}

func countFileMd5(filePath string) string {
	file, err := os.Open(filePath)
	if err != nil {
		return err.Error()
	}
	defer file.Close()

	info, _ := file.Stat()
	fileSize := info.Size()

	blocks := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	hash := md5.New()

	for i := uint64(0); i < blocks; i++ {
		blockSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		buf := make([]byte, blockSize)

		file.Read(buf)
		io.WriteString(hash, string(buf))
	}

	return fmt.Sprintf("%x", hash.Sum(nil))
}

func PathExists(path string) (bool, error) {
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

func (t *TestCase) BucketInitTest() (err error) {
	log.Info("ooooo 开始测试Bucket创建与删除 ooooo")
	err = t.Bucketer.MakeBucket(testBucketName)
	HandleErr("Make Bucket ", err)
	err = t.Bucketer.DeleteBucket(testBucketName)
	HandleErr("Delete Bucket ", err)
	log.Info("√√√√√ Bucket创建与删除测试完成 √√√√√")
	return nil
}

func (t *TestCase) FileTest() error {
	defer func() {
		if err := os.RemoveAll(testTmpPath); err != nil {
			log.Errorf("remove all: %s", err)
		}
		if err := os.RemoveAll(verifyTmpPath); err != nil {
			log.Errorf("remove all: %s", err)
		}
	}()
	t.Bucketer.MakeBucket(t.Config.Bucket)
	PathExists(testTmpPath)
	for i := 0; i < 8; i++ {
		log.Infof("正在生成文件大小为%.2fKB的文件", FileSize[i]/1024)
		CreateFixedFile(FileSize[i], fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i)))
		t.SrcMd5 = append(t.SrcMd5, countFileMd5(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i))))
	}
	log.Info("测试文件生成完成")
	for i := 0; i < 8; i++ {
		log.Infof("开始上传文件大小为%.2fKB的文件", FileSize[i]/1024)
		bT := time.Now() // 开始时间
		err := t.Uploader.Upload(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i)), fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i)), true)
		if err != nil {
			log.Fatal("上传文件失败", err)
		}
		eT := time.Since(bT) // 从开始到当前所消耗的时间
		log.Infof("上传文件成功 速度%.2f KB/s", FileSize[i]/eT.Seconds())
	}
	PathExists(verifyTmpPath)
	for i := 0; i < 8; i++ {
		t.Downloader.DownloadFile(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i)), fmt.Sprintf("%s/test_%s", verifyTmpPath, strconv.Itoa(i)))
		dstMd5 := countFileMd5(fmt.Sprintf("%s/test_%s", verifyTmpPath, strconv.Itoa(i)))
		log.Infof("√√√√√ 开始校验上传文件的MD5是否正确, MD5比较结果：%s/%s 校验通过 √√√√√", dstMd5, t.SrcMd5[i])
		if dstMd5 != t.SrcMd5[i] {
			log.Fatal("Md5 检测失败")
		}
	}
	log.Info("√√√√√文件上传下载一致性检测完成√√√√√")
	return nil
}

func (t *TestCase) DeleteTest() error {
	defer func() {
		if err := os.RemoveAll(testTmpPath); err != nil {
			log.Errorf("remove all: %s", err)
		}
	}()
	err := t.Bucketer.DeleteBucket(t.Config.Bucket)
	if err != nil {
		if find := strings.Contains(err.Error(), "Bucket not empty cannot delete"); find {
			log.Info("√√√√√ Bucket存在文件 禁止删除 测试通过 √√√√√")
		}
	} else {
		log.Fatal("Bucket存在文件 禁止删除 测试失败")
	}
	log.Info("ooooo Bucket删除文件 操作测试开始 ooooo")
	for i := 0; i < 8; i++ {
		err = t.Modify.DeleteFile(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Info("√√√√√ Bucket删除文件测试完成 √√√√√")
	log.Info("ooooo 非覆盖上传文件测试开始 ooooo")
	PathExists(testTmpPath)
	CreateFixedFile(FileSize[0], fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(0)))
	err = t.Uploader.Upload(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(0)), "overwrite_test", true)
	if err != nil {
		log.Fatal(err)
	}
	err = t.Uploader.Upload(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(0)), "overwrite_test", false)
	if err != nil {
		if find := strings.Contains(err.Error(), "obj already exist"); find {
			log.Info("发现上传失败报错 符合预期 校验成功")
			log.Info("√√√√√ 非覆盖上传文件测试完成 √√√√√")
		} else {
			log.Fatal(err)
		}
	} else {
		log.Fatal("非覆盖上传文件测试失败")
	}
	log.Info("ooooo 覆盖文件上传模式测试开始 ooooo")
	err = t.Uploader.Upload(fmt.Sprintf("%s/test_%s", testTmpPath, strconv.Itoa(0)), "overwrite_test", true)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("√√√√√ 覆盖文件上传模式测试完成 √√√√√")
	log.Info("ooooo 重命名文件测试开始 ooooo")
	log.Info("检测Bucket是否存在即将与重命名文件同名文件")
	req, err := t.Downloader.GetFileExiet("new_overwrite_test")
	if err != nil {
		log.Warn(err)
		log.Info("检测返回404 未发现同名文件 符合预期")

	}
	if req {
		err = t.Modify.DeleteFile("new_overwrite_test")
		if err != nil {
			log.Fatal(err)
		}
	}
	err = t.Modify.RenameFile("overwrite_test", "new_overwrite_test")
	if err != nil {
		log.Info(err)
		log.Fatal("重命名文件测试失败")
	}
	log.Info("√√√√√ 重命名文件测试完成 √√√√√")
	return nil
}

func runTestCase(ctx *cli.Context) error {
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
	var testCase = &TestCase{
		Config:     x,
		Uploader:   operation.NewUploader(x),
		Downloader: operation.NewDownloader(x),
		Bucketer:   operation.NewBucketer(x),
		Modify:     operation.NewModifier(x),
	}
	log.Info("√√√√√ 测试用例初始化完成 √√√√√")
	testCase.BucketInitTest()
	log.Info("ooooo开始进行文件功能测试ooooo")
	testCase.FileTest()
	log.Info("ooooo开始进行Bucket删除相关检测ooooo")
	testCase.DeleteTest()
	log.Info("开始执行相关测试清理工作")
	err = testCase.Modify.DeleteFile("new_overwrite_test")
	if err != nil {
		log.Fatal(err)
	}
	log.Info("清理测试Bucket测试文件成功")
	testCase.Bucketer.DeleteBucket(testCase.Config.Bucket)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("清理测试Bucket成功")
	log.Info("测试完成")
	return nil
}

var proveCmd = &cli.Command{
	Name:  "test",
	Usage: "Test Case",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "qiniu",
			Usage: "qiniu Env",
			Value: "./cfg_qiniu.toml",
		},
	},
	Action: runTestCase,
}

func main() {
	app := &cli.App{
		Name:    "test",
		Usage:   "Test Case",
		Version: "1.0.0",
		Commands: []*cli.Command{
			proveCmd,
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("%+v", err)
		return
	}
	//CreateFixedFile(10*1024*1024, "./small.txt")
}
