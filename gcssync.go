package main

import (
    "fmt"
    "os"
    //"log"
)

var (
    Version string
)

func usage() {
    fmt.Println("Usage:")
    fmt.Println("gcssync local_path gs://[backet name]/path")
}

func version() {
    fmt.Println("gcssync version: " + Version)
}

func main() {
    if len(os.Args) < 3 {
        version()
        usage()
        os.Exit(1)
    }
    
    hasError := false
    cred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
    projId := os.Getenv("GOOGLE_PROJECT_ID")
    srcDirName := os.Args[1]
    bucketName, path, err := getBucketName(os.Args[2])
    if err != nil {
        showError(err.Error())
        hasError = true
    }

    if cred == "" {
        showError("Environment Variable GOOGLE_APPLICATION_CREDENTIALS not specified")
        hasError = true
    }
    if projId == "" {
        showError("Environment Variable GOOGLE_PROJECT_ID not specified")
        hasError = true
    }

    if hasError {
        os.Exit(1)
    }

    bucket := NewGCSBucket(bucketName)
    if bucket == nil {
        showError("Cannot create GCSBucket object")
        os.Exit(1)
    }

    err = bucket.syncFiles(path, srcDirName)
    if err != nil {
        showError(err.Error())
        os.Exit(1)
    }
}

func showError(msg string) {
    fmt.Fprintln(os.Stderr, msg)
}

