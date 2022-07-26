package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/CQUST-Runner/datacross/storage"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WorkingDirectory string `yaml:"wd" default:"./data"`
	MachineName      string `yaml:"machine_name" default:"machine0"`
}

func loadConfig(filename string, c *Config) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, c)
}

var c *Config

func main() {
	confFile := ""
	flag.StringVar(&confFile, "conf", "conf.yaml", "config file path")
	flag.Parse()

	conf := Config{}
	err := loadConfig(confFile, &conf)
	if err != nil {
		fmt.Println("load conf file failed", err)
		return
	}
	fmt.Println("conf: ", conf)
	c = &conf

	participant := storage.Participant{}
	err = participant.Init(c.WorkingDirectory, c.MachineName)
	if err != nil {
		fmt.Println("init participant failed", err)
		return
	}
	defer participant.Close()

	s := Shell{}
	s.Init(os.Stdin, os.Stdout, &participant)
	s.Repl()
}
