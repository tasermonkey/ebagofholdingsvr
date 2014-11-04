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
	getFile        = ""
	dropDir        = "./bag"
)

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

func drainAndLogEvents(pipeline <-chan s3sync.DownloadEvent) {
	for evt := range pipeline {
		localFullPath := evt.LocalFileName
		download_time := time.Since(evt.StartTime)
		log.Printf("Finished %s (%s)", localFullPath, download_time.String())
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

func RunClientService() {
	start_time := time.Now()
	syncer := s3sync.MakeS3Sync()
	syncer.LocalPath = dropDir
	log.Println("eBag LocalPath: ", syncer.LocalPath)
	pipeline := createPipeline(syncer)
	drainAndLogEvents(pipeline)
	log.Println("Finished downloading: ", time.Since(start_time).String())
}

func parsecli() {
	flag.BoolVar(&restservice, "restservice", false, "Specify this flag if you want to run the rest service")
	flag.StringVar(&getFile, "get-file", "", "Specify this flag to download a file")
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
	RunClientService()

}
