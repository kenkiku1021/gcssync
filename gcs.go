package main

import (
    "crypto/md5"
    "bytes"
    //"fmt"
    "context"
    "errors"
    "io"
    "log"
    "os"
    "regexp"
    "strings"
    "cloud.google.com/go/storage"
)

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
    
    err := gcsBucket.deleteRemovedObjects(dstPath + "/", srcDirName)
    if err != nil {
        return err
    }

    err = gcsBucket.syncFilesInternal(dstPath, srcDirName)
    return err
}

func (gcsBucket *GCSBucket) syncFilesInternal(dstPath string, srcDirName string) error {
    srcDir, err := os.Open(srcDirName)
    if err != nil {
        return err
    }
    files, err := srcDir.Readdir(0)
    if err != nil {
        return err
    }

    filesCount := len(files)
    for i := 0; i < filesCount; i++ {
        file := files[i]
        fullpath := srcDirName + "/" + file.Name()
        target := strings.TrimLeft(dstPath + "/" + file.Name(), "/")
        if file.IsDir() {
            err = gcsBucket.syncFiles(target, fullpath)
            if err != nil {
                return err
            }
        } else {
            err = gcsBucket.syncFile(target, fullpath)
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func (gcsBucket *GCSBucket) syncFile(targetPath, srcPath string) error {
    targetObj := gcsBucket.bucket.Object(targetPath)
    attrs, err := targetObj.Attrs(gcsBucket.ctx)
    if err == nil {
        fileHashSum, err := md5Hash(srcPath)
        if err != nil {
            return err
        }
        
        if bytes.Compare(fileHashSum, attrs.MD5) == 0 {
            // file is not modified
            gcsBucket.logger.Printf("not modified: %s", srcPath)
            return nil
        }
    }

    f, err := os.Open(srcPath)
    if err != nil {
        return err
    }
    defer f.Close()

    gcsBucket.logger.Printf("upload: %s -> %s", srcPath, targetPath)
    wc := targetObj.NewWriter(gcsBucket.ctx)
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
