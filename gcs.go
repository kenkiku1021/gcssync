package main

import (
    "crypto/md5"
    "bytes"
    "fmt"
    "context"
    "errors"
    "io"
    "log"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "cloud.google.com/go/storage"
)

const MAX_GOROUTINE_COUNT = 4

func d(s string) {
    fmt.Fprintln(os.Stderr, s)
}

func getContentType(filename string) string {
    ext := filepath.Ext(filename)
    switch ext {
    case ".txt":
        return "text/plain"
    case ".html":
        return "text/html"
    case ".htm":
        return "text/html"
    case ".css":
        return "text/css"
    case ".js":
        return "text/javascript"
    case ".pdf":
        return "application/pdf"
    case ".svg":
        return "image/svg+xml"
    default:
        return ""
    }
}

func getBucketName(s string) (string, string, error) {
    re := regexp.MustCompile("^gs:\\/\\/([^\\/]+)\\/(.*)$")
    result := re.FindStringSubmatch(s)
    if len(result) == 0 {
        return "", "", errors.New("Invalid Google Cloud Storage URI")
    }

    return result[1], result[2], nil
}

type GCSBucket struct {
    ctx context.Context
    client *storage.Client
    bucket *storage.BucketHandle
    logger *log.Logger
}

type SyncInfo struct {
    fullpath string
    target string
}

func NewGCSBucket(bucketName string) *GCSBucket {
    var gcsBucket *GCSBucket
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    bucket := client.Bucket(bucketName)

    _, err = bucket.Attrs(ctx)
    if err != nil {
        showError("Bucket Error")
        showError(err.Error())
    } else {
        gcsBucket = &GCSBucket {
            ctx: ctx,
            client: client,
            bucket: bucket,
            logger: log.New(os.Stdout, "GCSBucket: ", log.LstdFlags),
        }
    }
    return gcsBucket
}

func (gcsBucket *GCSBucket) syncFiles(dstPath string, srcDirName string) error {
    srcDirName = strings.TrimRight(srcDirName, "/")
    dstPath = strings.TrimRight(dstPath, "/")
    
    err := gcsBucket.deleteRemovedObjects(dstPath + "/", srcDirName)
    if err != nil {
        return err
    }

    dirInfo := SyncInfo {
        fullpath: srcDirName,
        target: dstPath,
    }
    err = gcsBucket.syncFilesInternal(dirInfo)
    return err
}

func (gcsBucket *GCSBucket) syncFilesInternal(dirInfo SyncInfo) error {
    srcDir, err := os.Open(dirInfo.fullpath)
    if err != nil {
        return err
    }
    files, err := srcDir.Readdir(0)
    if err != nil {
        return err
    }

    gcsBucket.logger.Printf("syncing dir: %s -> %s", dirInfo.fullpath, dirInfo.target)
    
    filesCount := len(files)
    var subdirs []SyncInfo
    gcsBucket.logger.Printf("%s : %d files", dirInfo.fullpath, filesCount)
	for i := 0; i < filesCount; i++ {
        syncInfo := SyncInfo {
            fullpath: dirInfo.fullpath + "/" + files[i].Name(),
            target: strings.TrimLeft(dirInfo.target + "/" + files[i].Name(), "/"),
        }
        if files[i].IsDir() {
            subdirs = append(subdirs, syncInfo)
        } else {
            err = gcsBucket.syncFile(syncInfo)
            if err != nil {
                showError(err.Error())
                os.Exit(1)
            }
        }
    }

    subdirsCount := len(subdirs)
    for i := 0; i < subdirsCount; i++ {
        err = gcsBucket.syncFilesInternal(subdirs[i])
        if err != nil {
            showError(err.Error())
            os.Exit(1)
        }
    }

    return nil
}

func (gcsBucket *GCSBucket) syncFilesInternalGoRoutine(dirInfo SyncInfo) error {
    srcDir, err := os.Open(dirInfo.fullpath)
    if err != nil {
        return err
    }
    files, err := srcDir.Readdir(0)
    if err != nil {
        return err
    }

    gcsBucket.logger.Printf("syncing dir: %s -> %s", dirInfo.fullpath, dirInfo.target)
    
    var empty struct{}

    limit := make(chan struct{}, MAX_GOROUTINE_COUNT)
    filesCount := len(files)
    var subdirs []SyncInfo
    gcsBucket.logger.Printf("%s : %d files", dirInfo.fullpath, filesCount)
    for i := 0; i < filesCount; i++ {
        select {
        case limit <- empty:
            go func(file os.FileInfo) {
                syncInfo := SyncInfo {
                    fullpath: dirInfo.fullpath + "/" + file.Name(),
                    target: strings.TrimLeft(dirInfo.target + "/" + file.Name(), "/"),
                }
                if file.IsDir() {
                    subdirs = append(subdirs, syncInfo)
                } else {
                    err = gcsBucket.syncFile(syncInfo)
                    if err != nil {
                        showError(err.Error())
                        os.Exit(1)
                    }
                }
                <-limit
            }(files[i])
        }
    }

    subdirsCount := len(subdirs)
    for i := 0; i < subdirsCount; i++ {
        err = gcsBucket.syncFilesInternal(subdirs[i])
        if err != nil {
            showError(err.Error())
            os.Exit(1)
        }
    }

    return nil
}

func (gcsBucket *GCSBucket) syncFile(fileInfo SyncInfo) error {
    targetObj := gcsBucket.bucket.Object(fileInfo.target)
    attrs, err := targetObj.Attrs(gcsBucket.ctx)
    if err == nil {
        fileHashSum, err := md5Hash(fileInfo.fullpath)
        if err != nil {
            return err
        }
        
        if bytes.Compare(fileHashSum, attrs.MD5) == 0 {
            // file is not modified
            gcsBucket.logger.Printf("not modified: %s", fileInfo.fullpath)
            return nil
        }
    }

    f, err := os.Open(fileInfo.fullpath)
    if err != nil {
        return err
    }
    defer f.Close()

    gcsBucket.logger.Printf("upload: %s -> %s", fileInfo.fullpath, fileInfo.target)
    wc := targetObj.NewWriter(gcsBucket.ctx)
    ct := getContentType(fileInfo.fullpath)
    if ct != "" {
        wc.ContentType = ct
    }
    if _, err = io.Copy(wc, f); err != nil {
        return err
    }
    if err := wc.Close(); err != nil {
        return err
    }
    
    return nil
}

func (gcsBucket *GCSBucket) deleteRemovedObjects(prefix string, rootPath string ) error {
    var q *storage.Query
    if prefix == "" {
        q = nil
    } else {
        q = &storage.Query{Prefix: prefix, Delimiter: "/"}
    }

    rootPath = strings.TrimRight(rootPath, "/")

    it := gcsBucket.bucket.Objects(gcsBucket.ctx, q)
    attr, err := it.Next()
    for err == nil {
        path := rootPath + "/" + strings.TrimPrefix(attr.Name, prefix)
        if attr.Name != "" {
            _, err = os.Stat(path)
            if err != nil {
                // cannot stat file
                object := gcsBucket.bucket.Object(attr.Name)
                gcsBucket.logger.Printf("delete object: %s (local file %s is not exists)", attr.Name, path)
                err := object.Delete(gcsBucket.ctx)
                if err != nil {
                    return err
                }
            }
        }
        attr, err = it.Next()
    }

    return nil
}

func (gcsBucket *GCSBucket) getObjects(prefix string) ([]string) {
    gcsBucket.logger.Printf("prefix: %s", prefix)
    q := &storage.Query{Prefix: prefix, Delimiter: "/"}
    it := gcsBucket.bucket.Objects(gcsBucket.ctx, q)
    var objects []string

    attr, err := it.Next()
    for err == nil {
        objects = append(objects, attr.Name)
        gcsBucket.logger.Println(attr)
        attr, err = it.Next()
    }

    return objects
}

func (gcsBucket *GCSBucket) getAllObjects() ([]string) {
    it := gcsBucket.bucket.Objects(gcsBucket.ctx, nil)
    var objects []string

    attr, err := it.Next()
    for err == nil {
        objects = append(objects, attr.Name)
        gcsBucket.logger.Println(attr)
        attr, err = it.Next()
    }

    return objects
}

func md5Hash(path string) ([]byte, error) {
    f, err := os.Open(path)
    if(err != nil) {
        return nil, err
    }
    defer f.Close()
    
    hash := md5.New()
    if _, err := io.Copy(hash, f); err != nil {
        return nil, err
	}

    return hash.Sum(nil), nil
}
