package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/CQUST-Runner/datacross/storage"
)

type Shell struct {
	r io.Reader
	w io.Writer
	p *storage.Participant
}

func (s *Shell) Init(input io.Reader, output io.Writer, p *storage.Participant) {
	s.r = input
	s.w = output
	s.p = p
}

func (s *Shell) list(w io.Writer, args ...string) {
	records, err := s.p.All()
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	fmt.Fprintln(w, records)
}

func (s *Shell) get(w io.Writer, args ...string) {
	if len(args) < 1 {
		fmt.Fprintln(w, "too few args, usage: get <key>")
		return
	}

	key := args[0]
	val, err := s.p.Load(key)
	if err != nil {
		fmt.Fprintln(w, "get failed", err)
		return
	}
	fmt.Fprintln(w, val)
}

func (s *Shell) set(w io.Writer, args ...string) {
	if len(args) < 2 {
		fmt.Fprintln(w, "too few args, usage: set <key> <value>")
		return
	}

	key := args[0]
	value := args[1]
	err := s.p.Save(key, value)
	if err != nil {
		fmt.Fprintln(w, "set failed", err)
		return
	}
}

func (s *Shell) del(w io.Writer, args ...string) {
	if len(args) < 1 {
		fmt.Fprintln(w, "too few args, usage: del <key>")
		return
	}

	key := args[0]
	err := s.p.Del(key)
	if err != nil {
		fmt.Fprintln(w, "del failed", err)
		return
	}
}

func (s *Shell) has(w io.Writer, args ...string) {
	if len(args) < 1 {
		fmt.Fprintln(w, "too few args, usage: has <key>")
		return
	}

	key := args[0]
	has, err := s.p.Has(key)
	if err != nil {
		fmt.Fprintln(w, "has failed", err)
		return
	}
	fmt.Println(has)
}

func (s *Shell) conflicts(w io.Writer, args ...string) {
	conflicts, err := s.p.AllConflicts()
	if err != nil {
		fmt.Fprintln(w, "get conflicts failed", err)
		return
	}
	for _, c := range conflicts {
		if c == nil {
			continue
		}
		fmt.Fprintln(w, c)
	}
}

func (s *Shell) resolve(w io.Writer, args ...string) {
	if len(args) < 1 {
		fmt.Fprintln(w, "too few args, usage: resolve <key>")
		return
	}

	key := args[0]
	v, err := s.p.Load(key)
	if err != nil {
		fmt.Fprintf(w, "load key failed[%v]\n", err)
		return
	}

	if len(v.Branches()) == 0 {
		fmt.Fprintf(w, "key is not in conflict state, no action needed\n")
		return
	}

	for _, version := range v.Versions() {
		fmt.Fprintln(w, version)
	}

	fmt.Fprint(w, "enter seq num to accept(the num in the last column): ")
	var seq int
	_, err = fmt.Fscan(s.r, &seq)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	if !v.ValidSeq(seq) {
		fmt.Fprintf(w, "error, invalid seq[%v]\n", seq)
		return
	}

	err = s.p.Accept(v, seq)
	if err != nil {
		fmt.Fprintln(w, "error, ", err)
		return
	}
	fmt.Fprintln(w, "ok")
}

func (s *Shell) help(w io.Writer, args ...string) {
	fmt.Fprintln(w, `
list
get <key>
del <key>
has <key>
set <key> <value>
resolve <key>
conflicts
help
exit
	`)
}

func (s *Shell) exec(w io.Writer, cmd string) {
	cmd = strings.TrimSpace(cmd)
	comps := strings.Split(cmd, " ")
	tokens := []string{}
	for _, comp := range comps {
		if len(comp) > 0 {
			tokens = append(tokens, comp)
		}
	}
	if len(tokens) == 0 {
		return
	}

	switch tokens[0] {
	case "list":
		s.list(w, tokens[1:]...)
	case "get":
		s.get(w, tokens[1:]...)
	case "del":
		s.del(w, tokens[1:]...)
	case "has":
		s.has(w, tokens[1:]...)
	case "set":
		s.set(w, tokens[1:]...)
	case "resolve":
		s.resolve(w, tokens[1:]...)
	case "conflicts":
		s.conflicts(w, tokens[1:]...)
	case "help":
		s.help(w, tokens[1:]...)
	default:
		fmt.Printf("unknown command `%v`, type `help` for usage\n", tokens[0])
	}
}

func (s *Shell) Repl() {
	scanner := bufio.NewScanner(s.r)
	for {
		ok := scanner.Scan()
		if !ok {
			fmt.Println("read line error")
			continue
		}
		cmd := scanner.Text()

		if cmd == "exit" {
			break
		}

		s.exec(s.w, cmd)
	}
}
