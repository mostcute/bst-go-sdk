package main

import (
	"flag"
	"log"
	"stroage-go-sdk/operation"
)

func main() {
	cf := flag.String("c", "cfg_bst.toml", "config")
	flag.Parse()

	x, err := operation.Load(*cf)
	if err != nil {
		log.Fatalln(err)
	}

	down := operation.NewDownloader(x)
	data, err := down.DownloadBytes("test")
	if err != nil {
		log.Fatal(err)
		return
	}
	up := operation.NewUploader(x)
	err = up.UploadBytes(data, "ceshi", false)
	if err != nil {
		log.Fatal(err)
		return
	}

	down.DownloadFile("ceshi", "ceshi")

	//deleter := operation.NewDeleter(x)
	//deleter.DeleteFile("/root/test")

	//bucketer := operation.NewBucketer(x)
	////bucketer.MakeBucket("uutest")
	//status, err := bucketer.ListObject("uutest")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Printf("%v", status)
	//uploader := operation.NewUploader(x)
	//
	//uploader.Upload("go.mod", "/root/test", true)
	//
	//modify := operation.NewModifier(x)
	//modify.RenameFile("test2", "/root/ttest")

	//info, err := download.GeFileExiet("/root/test2")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Printf("%s", info)
}
