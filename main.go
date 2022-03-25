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

	uploader := operation.NewUploader(x)

	uploader.Upload("go.mod", "/root/test2", true)

	download := operation.NewDownloader(x)

	download.DownloadFile("/root/test2", "test.txt")

}
