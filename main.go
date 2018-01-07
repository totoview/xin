package main

//go:generate protoc --proto_path=pb pb/base.proto --go_out=plugins=grpc:pb
//go:generate protoc --proto_path=pb pb/mgw/mgw.proto --go_out=plugins=grpc,Mbase.proto=github.com/totoview/xin/pb:pb
//go:generate protoc --proto_path=pb pb/store/store.proto --go_out=plugins=grpc,Mbase.proto=github.com/totoview/xin/pb:pb
//go:generate go run generate.go

import (
	"fmt"
	"os"

	"github.com/totoview/xin/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
