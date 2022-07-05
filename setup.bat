@REM nodejs npm angular
@REM go mingw
@REM npm install -g pkg

cd frontend
npm install
cd ..
cd third-party/single-file-cli
npm install

go install github.com/gogo/protobuf/protoc-gen-gofast@latest
