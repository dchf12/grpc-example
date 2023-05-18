package main

import (
	"context"
	"fmt"
	"grpc-example/pb"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	certFile := "/Users/dch/Library/Application Support/mkcert/rootCA.pem"
	creds, err := credentials.NewClientTLSFromFile(certFile, "")
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)
	// callListFiles(client)
	callDownload(client)
	// callUpload(client)
	// callUploadAndNotifyProgress(client)
}

func callListFiles(client pb.FileServiceClient) {
	md := metadata.New(map[string]string{"authorization": "Bearer bad-token"})
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	res, err := client.ListFiles(ctx, &pb.ListFilesRequest{})
	if err != nil {
		log.Fatalf("could not list files: %v", err)
	}
	log.Printf("Files: %v", res.GetFilenames())
}

func callDownload(client pb.FileServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.DownloadRequest{Filename: "name.txt"}
	stream, err := client.Download(ctx, req)
	if err != nil {
		log.Fatalf("could not download file: %v", err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			resErr, ok := status.FromError(err)
			if ok && resErr.Code() == codes.NotFound {
				log.Fatalf("Error Code: %v, Error Message: %v", resErr.Code(), resErr.Message())
			} else if resErr.Code() == codes.DeadlineExceeded {
				log.Fatalln("deadline exceeded")

			} else {
				log.Fatalln("unknown grpc error")
			}
			log.Fatalf("could not receive file data: %v", err)
		}
		log.Printf("Respose from Download(bytes): %v", res.GetData())
		log.Printf("Respose from Download(string): %v", string(res.GetData()))
	}
}

func callUpload(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := fmt.Sprintf("./storage/%s", filename)

	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	stream, err := client.Upload(context.Background())
	if err != nil {
		log.Fatalf("could not upload file: %v", err)
	}
	buf := make([]byte, 5)
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("could not read file: %v", err)
		}
		req := &pb.UploadRequest{Data: buf[:n]}
		if err := stream.Send(req); err != nil {
			log.Fatalf("could not send file data: %v", err)
		}
		time.Sleep(1 * time.Second)

	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("could not receive response: %v", err)
	}
	log.Printf("received data size: %v", res)
}

func callUploadAndNotifyProgress(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := fmt.Sprintf("./storage/%s", filename)

	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	stream, err := client.UploadAndNotifyProgress(context.Background())
	if err != nil {
		log.Fatalf("could not upload file: %v", err)
	}

	// request
	buf := make([]byte, 5)
	go func() {
		for {
			n, err := file.Read(buf)
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("could not read file: %v", err)
			}
			req := &pb.UploadAndNotifyProgressRequest{Data: buf[:n]}
			if err := stream.Send(req); err != nil {
				log.Fatalf("could not send file data: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
		err := stream.CloseSend()
		if err != nil {
			log.Fatalf("could not close send: %v", err)
		}
	}()

	// response
	ch := make(chan struct{})
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("could not receive response: %v", err)
			}
			log.Printf("received message: %v", res.GetMsg())
		}
		close(ch)
	}()
	<-ch
}
