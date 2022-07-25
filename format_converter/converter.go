package main

import (
	"flag"
	"fmt"

	"github.com/CQUST-Runner/datacross/storage"
)

func main() {
	input := flag.String("input", "", "")
	format := flag.String("format", "", "")
	output := flag.String("output", "", "")
	flag.Parse()

	fmt.Println(*input, *format, *output)

	var iff storage.LogFormat
	var of storage.LogFormat

	if *format == "json" {
		iff = &storage.JsonLog{}
		of = &storage.BinLog{}
	} else {
		iff = &storage.BinLog{}
		of = &storage.JsonLog{}
	}

	wi := storage.Wal{}
	if err := wi.Init(*input, iff, true); err != nil {
		fmt.Println(err)
		return
	}
	defer wi.Close()
	wo := storage.Wal{}
	if err := wo.Init(*output, of, false); err != nil {
		fmt.Println(err)
		return
	}
	defer wo.Close()

	// TODO use per entry iteration
	it := wi.Iterator()
	for it.Next() {
		err := wo.AppendRaw(it.LogOp())
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
