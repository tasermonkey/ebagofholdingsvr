package main

import (
	"flag"
	"github.com/emicklei/go-restful"
	//"github.com/kr/pretty"
	"github.com/tasermonkey/ebagofholdinglib/s3sync"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// This example shows the minimal code needed to get a restful.WebService working.
//
// GET http://localhost:8080/hello

var (
	WORKING_DIR, _ = os.Getwd()
	restservice    = false
	uploadChanges  = false
	dropDir        = "./bag"
)

func getFileSize(filepath string) (filesize int64, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	defer CheckClose(file, &err)
	stat, err := file.Stat()
	if err != nil {
		return
	}
	filesize = stat.Size()
	return
}

func CheckClose(c io.Closer, errp *error) {
	err := c.Close()
	if err != nil && *errp == nil {
		*errp = err
	}
}

func RunRestService() {
	ws := new(restful.WebService)
	// ws.Route(ws.GET("/hello").To(hello))
	restful.Add(ws)
	http.ListenAndServe(":8080", nil)
}

func printError(err *error) {
	if err != nil {
		log.Fatal(*err)
	}
}

func logStartofDownload(incoming <-chan s3sync.DownloadEvent) <-chan s3sync.DownloadEvent {
	output := make(chan s3sync.DownloadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			key := evt.RemoteItem
			localFullPath := evt.LocalFileName
			output <- evt
			log.Printf("Starting %s (%d bytes)", localFullPath, key.Size)
		}
	}()
	return output
}

func logStartofUpload(incoming <-chan s3sync.UploadEvent) <-chan s3sync.UploadEvent {
	output := make(chan s3sync.UploadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			localFullPath := evt.LocalFileName
			output <- evt
			fileSize, err := getFileSize(localFullPath)
			if err != nil {
				log.Printf("Upload error %s: %s", localFullPath, err.Error())
			} else {
				log.Printf("Starting upload %s (%d bytes)", localFullPath, fileSize)
			}
		}
	}()
	return output
}

func drainAndLogEvents(pipeline <-chan s3sync.DownloadEvent) {
	for evt := range pipeline {
		localFullPath := evt.LocalFileName
		upload_time := time.Since(evt.StartTime)
		log.Printf("Finished %s (%s)", localFullPath, upload_time.String())
	}
}

func drainAndLogUploadEvents(pipeline <-chan s3sync.UploadEvent) {
	for evt := range pipeline {
		localFullPath := evt.LocalFileName
		download_time := time.Since(evt.StartTime)
		log.Printf("Finished upload %s (%s)", localFullPath, download_time.String())
	}
}

func createPipeline(syncer s3sync.S3Sync) <-chan s3sync.DownloadEvent {
	lister := syncer.ReadListing()
	etagFilter := syncer.FilterAlreadyDownloaded(lister)
	logStartDownload := logStartofDownload(etagFilter)
	downloader := syncer.DownloadItems(logStartDownload)
	etagWriter := syncer.WriteETagData(downloader)
	return etagWriter
}

func createUploadPipeline(syncer s3sync.S3Sync) <-chan s3sync.UploadEvent {
	dirWatcher, err := syncer.WatchDir()
	if err != nil {
		log.Panic(err.Error())
		return nil
	}
	remoteFileNamer := syncer.RemoteFileNamer(dirWatcher)
	logStartUpload := logStartofUpload(remoteFileNamer)
	fileUploader := syncer.UploadItems(logStartUpload)
	return fileUploader
}

func makeSyncer() s3sync.S3Sync {
	syncer := s3sync.MakeS3Sync()
	syncer.LocalPath = dropDir
	log.Println("eBag LocalPath: ", syncer.LocalPath)
	return syncer
}

func RunClientService() {
	start_time := time.Now()
	syncer := makeSyncer()
	pipeline := createPipeline(syncer)
	drainAndLogEvents(pipeline)
	log.Println("Finished downloading: ", time.Since(start_time).String())
}

func RunUploadService() {
	start_time := time.Now()
	syncer := makeSyncer()
	pipeline := createUploadPipeline(syncer)
	drainAndLogUploadEvents(pipeline)
	log.Println("Finished running: ", time.Since(start_time).String())
}

func parsecli() {
	flag.BoolVar(&restservice, "restservice", false, "Specify this flag if you want to run the rest service")
	flag.BoolVar(&uploadChanges, "watch-for-changes", false, "watch for changes and upload to s3")
	flag.StringVar(&dropDir, "drop-dir", dropDir, "Specify the drop-dir for files, default working directory")
	flag.Parse()
}

func main() {
	parsecli()
	dropDir, _ = filepath.Abs(dropDir)
	if restservice {
		RunRestService()
		return
	}
	if uploadChanges {
		RunUploadService()
		return
	}
	RunClientService()

}
