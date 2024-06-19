package main

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	client *s3.S3
	wg     sync.WaitGroup
)

func init() {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			"test", "test", ""), // LocalStack default credentials
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String("http://localhost:4566"), // LocalStack endpoint
		S3ForcePathStyle: aws.Bool(true),                      // Needed for LocalStack
	})

	if err != nil {
		panic(err)
	}
	log.Println("Sessão criada com sucesso!")
	client = s3.New(sess)
}

func main() {
	dir, err := os.Open("/tmp/")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	stopLight := make(chan struct{}, 1000)

	for {
		files, err := dir.ReadDir(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fileInfo, err := files[0].Info()
		if err != nil {
			fmt.Printf("Error getting file info: %v\n", err)
			continue
		}
		wg.Add(1)
		stopLight <- struct{}{}

		go uploadFile(fileInfo, stopLight)
	}
	wg.Wait()
}
func uploadFile(fileInfo os.FileInfo, stopLight <-chan struct{}) {
	defer wg.Done()

	fmt.Printf("Upload Started: %s\n", fileInfo.Name())

	filePath := fmt.Sprintf("/tmp/%s", fileInfo.Name())

	file, err := os.Open(filePath)
	if err != nil {
		<-stopLight
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer file.Close()

	var fileSize int64 = fileInfo.Size()

	fileBuffer := make([]byte, fileSize)
	file.Read(fileBuffer)

	_, err = client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("golang-files"),
		Key:    aws.String(fileInfo.Name()),
		Body:   bytes.NewReader(fileBuffer),
	})

	if err != nil {
		<-stopLight
		fmt.Printf("Error upload: %v\n", err)
		return
	}
	fmt.Printf("Upload Finish: %s\n", fileInfo.Name())
	<-stopLight
}
