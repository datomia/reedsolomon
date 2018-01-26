//+build ignore

// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple encoder example
//
// The encoder encodes a simgle file into a number of shards
// To reverse the process see "simpledecoder.go"
//
// To build an executable use:
//
// go build simple-decoder.go
//
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't diviable by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
//
// If you save these properties, you should abe able to detect file corruption
// in a shard and be able to reconstruct your data if you have the needed number of shards left.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reedsolomon.git/trunk"
	//"github.com/klauspost/reedsolomon"
)

var dataShards = flag.Int("data", 4, "Number of shards to split the data into, must be below 257.")
var parShards = flag.Int("par", 2, "Number of parity shards")
var outDir = flag.String("out", "", "Alternative output directory")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm;
// otherwise WriteFile truncates it before writing.
func WriteSubshardsIntoFile(filename string, data [][]byte, alpha int, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	for i:=0; i<alpha; i++ {
		/*n, err := */f.Write(data[i])
		/*if err == nil && n < len(data[i]) {
			err = io.ErrShortWrite
		}*/
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func main() {
	// Parse command line parameters.
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	if *dataShards > 257 {
		fmt.Fprintf(os.Stderr, "Error: Too many data shards\n")
		os.Exit(1)
	}
	fname := args[0]

	/*ext := byte(8)
	RS_Enc := reedsolomon.InitGaloisField(ext)
	RS_Enc.PrintGaloisField()
	if ext == 8 {
		RS_Enc.CompareGaloisFields256()
	}*/

	// Create HashTagCodec
	alpha := 4 // some value for the parameter
	encH, err := reedsolomon.NewHashTagCode(*dataShards, *parShards, alpha)

	fmt.Println("Opening", fname)
	b, err := ioutil.ReadFile(fname)
	checkErr(err)

	shards, err := encH.Split(b)
	checkErr(err)
	fmt.Printf("File split into %d (data+parity)*alpha subshards with %d bytes/subshard.\n", len(shards), len(shards[0]))

	err = encH.Encode(shards)
	checkErr(err)

	// Create encoding matrix.
	/*enc, err := reedsolomon.New(*dataShards, *parShards)
	checkErr(err)

	fmt.Println("Opening", fname)
	b, err := ioutil.ReadFile(fname)
	checkErr(err)

	// Split the file into equally sized shards.
	shards, err := enc.Split(b)
	checkErr(err)
	fmt.Printf("File split into %d data+parity shards with %d bytes/shard.\n", len(shards), len(shards[0]))

	// Encode parity
	err = enc.Encode(shards)
	checkErr(err)*/

	// Write out the resulting files.
	dir, file := filepath.Split(fname)
	if *outDir != "" {
		dir = *outDir
	}
	for i := 0; i<*dataShards+*parShards; i++ /*i, shard := range shards*/ {
		outfn := fmt.Sprintf("%s.%d", file, i)

		fmt.Println("Writing to", outfn)
		err = WriteSubshardsIntoFile(filepath.Join(dir, outfn), shards[i*alpha:(i+1)*alpha], alpha, os.ModePerm) // ???
		//err = ioutil.WriteFile(filepath.Join(dir, outfn), shard, os.ModePerm)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
		os.Exit(2)
	}
}
