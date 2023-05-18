package main

import (
	"bytes"
	"context"
	"fmt"
	"grpc-example/pb"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedFileServiceServer
}

func (s *server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	fmt.Println("ListFiles called")
	dir := "./storage"
	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var filenames []string
	for _, path := range paths {
		if !path.IsDir() {
			filenames = append(filenames, path.Name())
		}
	}
	return &pb.ListFilesResponse{Filenames: filenames}, nil
}

func (*server) Download(req *pb.DownloadRequest, stream pb.FileService_DownloadServer) error {
	fmt.Println("Download called")
	filename := req.GetFilename()
	path := fmt.Sprintf("./storage/%s", filename)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 5)
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.DownloadResponse{Data: buf[:n]}); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (*server) Upload(stream pb.FileService_UploadServer) error {
	fmt.Println("Upload called")

	var buf bytes.Buffer
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			res := &pb.UploadResponse{Size: int32(buf.Len())}
			return stream.SendAndClose(res)
		}
		if err != nil {
			return err
		}
		data := req.GetData()
		log.Printf("received data(bytes): %v", data)
		log.Printf("received data(string): %v", string(data))
		buf.Write(data)
	}
}

func (*server) UploadAndNotifyProgress(stream pb.FileService_UploadAndNotifyProgressServer) error {
	fmt.Println("UploadAndNotifyProgress called")

	size := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		data := req.GetData()
		log.Printf("received data(bytes): %v", data)
		log.Printf("received data(string): %v", string(data))
		size += len(data)
		res := &pb.UploadAndNotifyProgressResponse{
			Msg: fmt.Sprintf("received %d bytes", size),
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func myLogging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		log.Printf("request data: %+v", req)
		resp, err = handler(ctx, req)
		if err != nil {
			return nil, err
		}
		log.Printf("response data: %+v", resp)
		return resp, nil
	}
}

func authorize(ctx context.Context) (context.Context, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	if token != "test-token" {
		return nil, status.Error(codes.Unauthenticated, "token is invalid")
	}
	return ctx, nil
}

func main() {
	listen, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	creds, err := credentials.NewServerTLSFromFile(
		"ssl/localhost.pem",
		"ssl/localhost-key.pem",
	)
	if err != nil {
		log.Fatalf("Failed to generate credentials %v", err)
	}

	s := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				myLogging(),
				grpc_auth.UnaryServerInterceptor(authorize),
			),
		))
	pb.RegisterFileServiceServer(s, &server{})

	fmt.Println("Server is running...")
	if err := s.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
