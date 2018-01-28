package reedsolomon

import (
	"fmt"
	"errors"
	"os"
)

func (r HashTag) Reconstruct(fname string, subshardSize int, shards [][]byte) error {
	pIfFailedSN := make([]bool,r.Shards)
	for i:=0;i<r.Shards*r.Alpha;i++ {
		shards[i] = make([]byte, subshardSize)
	}
	readNum := 0
	decodingIsNeeded := false
	for i:=0;i<r.Shards;i++ {
		infn := fmt.Sprintf("%s.%d", fname, i)
		fmt.Println("Opening", infn)
		f, err := os.Open(infn)
		if err != nil {
			fmt.Printf("Error reading file %s\n",infn)
			pIfFailedSN[i] = true
			if i < r.DataShards {
				decodingIsNeeded = true
			}
			continue
		}
		if readNum < r.DataShards {
			for j:=0;j<r.Alpha;j++ {
				// whence: 0 means relative to the origin of the file, 1 means relative to the current offset, and 2 means relative to the end
				_, err := f.Seek(int64(j*subshardSize), 0)
				if err != nil {
					return errors.New(fmt.Sprintf("Error seeking position %d in file %s",j*subshardSize,infn))
				}
				fmt.Printf("Reading %d-th subchunk\n",j)
				_, err = f.Read(shards[i*r.Alpha+j])
				if err != nil {
					return errors.New(fmt.Sprintf("Error reading %d-th subshard in file %s",j,infn))
				}
			}
			readNum++
		}
		f.Close()
	}
	if decodingIsNeeded {
		if readNum < r.DataShards {
			fmt.Println("Data reconstruction is impossible.")
			fmt.Printf("Only %d storage nodes are available, but at least %d storage nodes are required.",readNum,r.DataShards)
			return nil
		} else {
			// perform decosing
		}
	} else {
		fmt.Println("Decoding is not required.\n")
	}
	return nil
}
