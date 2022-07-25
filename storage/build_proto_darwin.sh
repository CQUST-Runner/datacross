#!/bin/bash
gopath=`go env | grep GOPATH | cut -d'"' -f 2`"/bin"
export PATH=$gopath:$PATH
../third-party/protoc-21.2-osx-universal_binary/bin/protoc --gofast_out=. proto.proto
