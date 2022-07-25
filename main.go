package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
)

type httpHandler struct {
}

func serveSite(w http.ResponseWriter, req *http.Request) {
	p := req.URL.Path
	p = strings.TrimSpace(p)
	p = path.Clean(p)
	if len(p) == 0 || p == "/" {
		f, err := ioutil.ReadFile("../frontend/dist/frontend/index.html")
		if err != nil {
			fmt.Fprintln(w, "read file error", err)
			fmt.Fprintln(os.Stderr, "read file error", err)
			return
		}
		w.Write(f)
	} else {
		if path.Ext(p) == ".js" {
			w.Header().Set("Content-Type", "text/javascript")
		}
		p = path.Join("../frontend/dist/frontend", p)
		fmt.Printf("loading %v", p)
		f, err := ioutil.ReadFile(p)
		if err != nil {
			fmt.Fprintln(w, "read file error", err)
			fmt.Fprintln(os.Stderr, "read file error", err)
			return
		}
		w.Write(f)
	}
}

func serveAPI(w http.ResponseWriter, req *http.Request) {
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Println(req.URL)
	path := req.URL.Path
	isAPIRequest := strings.HasPrefix(path, "api") ||
		strings.HasPrefix(path, "/api")
	if isAPIRequest {
		serveAPI(w, req)
	} else {
		serveSite(w, req)
	}
}

func main() {
	h := httpHandler{}
	err := http.ListenAndServe("127.0.0.1:16224", &h)
	if err != nil {
		fmt.Println(err)
		return
	}

	ch := make(chan struct{})
	<-ch
}
