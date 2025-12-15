package process

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"time"
)

func V1(inpPath string) error {
	runtime.GOMAXPROCS(1)

	err := processV1(inpPath)
	if err != nil {
		return err
	}

	return nil
}

func processV1(inpPath string) error {
	fmt.Printf("Processing using V1: %s\n", inpPath)

	tr1 := time.Now().UnixMicro()
	err := readV1(inpPath)
	if err != nil {
		return err
	}
	tr2 := time.Now().UnixMicro()

	tc1 := time.Now().UnixMicro()
	err = calcV1()
	if err != nil {
		return err
	}
	tc2 := time.Now().UnixMicro()

	// prettyStoreV1()
	fmt.Printf("Finished Reading in: %d milliseconds\n", (tr2-tr1)/1_000)
	fmt.Printf("Finished Calculating in: %d milliseconds\n", (tc2-tc1)/1_000)
	fmt.Printf("Finished in: %d milliseconds\n", (tc2-tr1)/1_000)
	return nil
}

type statsV1 struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

var storeV1 = make(map[string]*statsV1)
var stationsV1 = make([]string, 0)

func readV1(inpPath string) error {
	f, err := os.Open(inpPath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)

	var line []byte

	var val float64
	var ok bool
	var history *statsV1

	for scanner.Scan() {
		line = scanner.Bytes()
		i := 0
		for i < len(line) && line[i] != 59 {
			i++
		}

		if val, err = strconv.ParseFloat(string(line[i+1:]), 64); err != nil {
			return err
		}

		history, ok = storeV1[string(line[:i])]
		if !ok {
			history = &statsV1{
				Min: val,
				Max: val,
			}
			storeV1[string(line[:i])] = history
			stationsV1 = append(stationsV1, string(line[:i]))
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

func calcV1() error {
	slices.Sort(stationsV1)
	for _, station := range stationsV1 {
		history := storeV1[station]
		// fmt.Printf("%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
		fmt.Fprintf(io.Discard, "%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
	}

	return nil
}

func prettyStoreV1() error {
	a, err := json.MarshalIndent(storeV1, "", " ")
	if err != nil {
		return err
	}
	fmt.Println(string(a))
	return nil
}
