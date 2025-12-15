package main

import (
	"1brc/process"
	"flag"
	"fmt"
)

func main() {
	fmt.Printf("Let's attempt the 1BRC challenge.\n")

	inpPath := flag.String("input", "", "the relative path of the input.txt file")
	version := flag.String("version", "", "the version number to run eg. 1/2/3...")
	flag.Parse()

	if *inpPath == "" {
		fmt.Println("no input file given")
		return
	}
	if *version == "" {
		fmt.Println("no version number given")
		return
	}
	var err error
	switch *version {
	case "1":
		err = process.V1(*inpPath)
	case "2":
		err = process.V2(*inpPath)
	default:
		err = fmt.Errorf("unknown version number")
	}
	if err != nil {
		fmt.Println(err)
		return
	}
}
