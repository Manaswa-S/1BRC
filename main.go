package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"time"
)

func main() {
	runtime.GOMAXPROCS(1)
	fmt.Printf("Let's attempt the 1BRC challenge.\n")

	inpPath := flag.String("input", "", "the relative path of the input.txt file")
	flag.Parse()

	if *inpPath == "" {
		fmt.Println("no input file given")
		return
	}

	err := Process(*inpPath)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func Process(inpPath string) error {
	fmt.Printf("Preparing to process %s\n", inpPath)

	tr1 := time.Now().UnixMicro()
	err := read(inpPath)
	if err != nil {
		return err
	}
	tr2 := time.Now().UnixMicro()

	tc1 := time.Now().UnixMicro()
	err = calc()
	if err != nil {
		return err
	}
	tc2 := time.Now().UnixMicro()

	// prettyStore()
	fmt.Printf("Finished Reading in: %d milliseconds\n", (tr2-tr1)/1_000)
	fmt.Printf("Finished Calculating in: %d milliseconds\n", (tc2-tc1)/1_000)
	fmt.Printf("Finished in: %d milliseconds\n", (tc2-tr1)/1_000)
	return nil
}

type stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

var store = make(map[string]*stats)
var stations = make([]string, 0)

func read(inpPath string) error {
	f, err := os.Open(inpPath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)

	var line []byte

	var val float64
	var ok bool
	var history *stats

	for scanner.Scan() {
		line = scanner.Bytes()
		i := 0
		for i < len(line) && line[i] != 59 {
			i++
		}

		if val, err = strconv.ParseFloat(string(line[i+1:]), 64); err != nil {
			return err
		}

		history, ok = store[string(line[:i])]
		if !ok {
			history = &stats{
				Min: val,
				Max: val,
			}
			store[string(line[:i])] = history
			stations = append(stations, string(line[:i]))
		}
		if history.Min > val {
			history.Min = val
		}
		if history.Max < val {
			history.Max = val
		}
		history.Sum += val
		history.Count += 1
	}

	return nil
}

func calc() error {
	slices.Sort(stations)
	for _, station := range stations {
		history := store[station]
		// fmt.Printf("%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
		fmt.Fprintf(io.Discard, "%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
	}

	return nil
}

func prettyStore() error {
	a, err := json.MarshalIndent(store, "", " ")
	if err != nil {
		return err
	}
	fmt.Println(string(a))
	return nil
}
