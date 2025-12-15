package process

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"time"
)

func V2(inpPath string) error {
	runtime.GOMAXPROCS(1)

	err := processv2(inpPath)
	if err != nil {
		return err
	}

	return nil
}

func processv2(inpPath string) error {
	fmt.Printf("Processing using V2: %s\n", inpPath)

	tr1 := time.Now().UnixMicro()
	err := readV2(inpPath)
	if err != nil {
		return err
	}
	tr2 := time.Now().UnixMicro()

	tc1 := time.Now().UnixMicro()
	err = calcV2()
	if err != nil {
		return err
	}
	tc2 := time.Now().UnixMicro()

	// prettyStoreV2()
	fmt.Printf("Finished Reading in: %d milliseconds\n", (tr2-tr1)/1_000)
	fmt.Printf("Finished Calculating in: %d milliseconds\n", (tc2-tc1)/1_000)
	fmt.Printf("Finished in: %d milliseconds\n", (tc2-tr1)/1_000)
	return nil
}

type statsV2 struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
	Name  string
	Hash  uint64
}

var storeV2 = make(map[uint64]*statsV2)
var stationsV2 = make([]string, 0)

func readV2(inpPath string) error {
	f, err := os.Open(inpPath)
	if err != nil {
		return err
	}

	// 16384 16MB
	// 4096 4MB
	// 1048576 1024MB
	// 32768 32MB
	// 24576 24MB
	var bufferSize = int(32768)
	var buffer = make([]byte, bufferSize)
	var n int

	var history *statsV2
	var ok bool
	var val float64

	var readStart int = 0

	var chunk []byte
	var remaining []byte

	var IN_NAME bool = true
	var namestart int = 0
	var colon int = 0
	var lastValid int = 0

	for {
		n, err = f.Read(buffer[readStart:])
		if err != nil && err != io.EOF {
			return err
		}

		chunk = buffer[:readStart+n]
		lastValid = len(chunk) - 1
		for ; lastValid >= 0; lastValid-- {
			if chunk[lastValid] == 10 {
				break
			}
		}
		if lastValid == -1 {
			break
		}

		remaining = chunk[lastValid+1:]
		chunk = chunk[:lastValid+1]

		namestart = 0
		colon = 0

		for i := 0; i < len(chunk); i++ {
			if IN_NAME && chunk[i] == 59 {
				IN_NAME = false
				colon = i
			} else if chunk[i] == 10 {
				// fmt.Printf("%s;%s\n", name, valStr)
				if val, err = strconv.ParseFloat(string(chunk[colon+1:i]), 64); err != nil {
					panic(err.Error())
				}

				hash := hash64(chunk[namestart:colon])
				history, ok = storeV2[hash]
				if !ok {
					history = &statsV2{
						Min:  val,
						Max:  val,
						Name: string(chunk[namestart:colon]),
					}
					storeV2[hash] = history
					stationsV2 = append(stationsV2, history.Name)
				}
				if history.Min > val {
					history.Min = val
				}
				if history.Max < val {
					history.Max = val
				}
				history.Sum += val
				history.Count += 1

				namestart = i + 1
				IN_NAME = true
			}
		}

		copy(buffer, remaining)
		readStart = len(remaining)
	}

	// fmt.Println(namestart, val, colon)

	return nil
}

func hash64(b []byte) uint64 {
	var h uint64 = 1469598103934665603
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func calcV2() error {
	slices.Sort(stationsV2)
	for _, station := range stationsV2 {
		history := storeV2[hash64([]byte(station))]
		// fmt.Printf("%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
		fmt.Fprintf(io.Discard, "%s;%.1f;%.1f;%.1f\n", station, history.Min, history.Max, (history.Sum / float64(history.Count)))
	}

	return nil
}

func prettyStoreV2() error {
	a, err := json.MarshalIndent(storeV2, "", " ")
	if err != nil {
		return err
	}
	fmt.Println(string(a))
	return nil
}
