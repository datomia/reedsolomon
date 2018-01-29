/**
 * HashTag Coding over 8-bit values.
 */

// Package HashTag enables Erasure Coding in Go
//
package reedsolomon

import (
	"fmt"
	"strings"
	"os"
	"strconv"
	"io/ioutil"
	"io"
	"errors"
)

// Encoder is an interface to encode HashTagCode parity sets for your data.
type HashTagCodec interface {
	// Encodes parity for a set of data shards.
	// Input is 'shards' containing data shards followed by parity shards.
	// The number of shards must match the number given to New().
	// Each shard is a byte array, and they must all be the same size.
	// The parity shards will always be overwritten and the data shards
	// will remain the same, so it is safe for you to read from the
	// data shards while this is running.
	Encode(shards [][]byte) error

	// Verify returns true if the parity shards contain correct data.
	// The data is the same format as Encode. No data is modified, so
	// you are allowed to read from data while this is running.
//	Verify(shards [][]byte) (bool, error)

	// Reconstruct will recreate the missing shards if possible.
	// If idxs argument is specified then only shards at specified indexes will be reconstructed.
	//
	// Given a list of shards, some of which contain data, fills in the
	// ones that don't have data.
	//
	// The length of the array must be equal to the total number of shards.
	// You indicate that a shard is missing by setting it to nil.
	//
	// If there are too few shards to reconstruct the missing
	// ones, ErrTooFewShards will be returned.
	//
	// The reconstructed shard set is complete, but integrity is not verified.
	// Use the Verify function to check if data set is ok.
	Repair(fname string, pIfFailedSN []bool, subshardSize int64, shards [][]byte) error
	Reconstruct(fname string, subshardSize int64, shards [][]byte) error
//	Reconstruct(shards [][]byte, idxs ...int) error

	// Split a data slice into the number of subshards given to the encoder,
	// and create empty parity subshards.
	//
	// The data will be split into equally sized shards.
	// If the data size isn't dividable by the number of shards,
	// the last shard will contain extra zeros.
	//
	// There must be at least 1 byte otherwise ErrShortData will be
	// returned.
	//
	// The data will not be copied, except for the last shard, so you
	// should not modify the data of the input slice afterwards.
	Split(data []byte) ([][]byte, error)

	// Join the shards and write the data segment to dst.
	//
	// Only the data shards are considered.
	// You must supply the exact output size you want.
	// If there are to few shards given, ErrTooFewShards will be returned.
	// If the total data size is less than outSize, ErrShortData will be returned.
	Join(dst io.Writer, shards [][]byte, outSize int) error
	GetNumOfSubchunksInChunk() int
}

// HashTag contains a matrices of indices and coefficients.
type HashTag struct {
	DataShards   int // Number of data shards, should not be modified.
	ParityShards int // Number of parity shards, should not be modified.
	Shards       int // Total number of shards. Calculated, and should not be modified.
	Alpha        int
	KDivR        int
	ppIndexArrayP             [][]int
	ppCoefficients            [][]byte
	ppPartitions              [][]int
	// for repair
	//FailedNodeID              int
	//NumOfExpr                 int
	pExpressionLength         []int
	ppExpressionElements      [][]int
	ppExpressionCoefficients  [][]byte
}

func (r HashTag) GetNumOfSubchunksInChunk() int {
	return r.Alpha
}

// ErrInvShardNum will be returned by New, if you attempt to create
// an HashTagCodec where either data or parity shards is zero or less.
//var ErrInvShardNum = errors.New("cannot create HashTagCodec with zero or less data/parity shards")

//var ErrShardSize = errors.New("shard size must be divisible by alpha")

// ErrMaxShardNum will be returned by New, if you attempt to create
// an HashTagCodec where data and parity shards cannot be bigger than
// Galois field GF(2^8) - 1.
//var ErrMaxShardNum = errors.New("cannot create HashTagCodec with 255 or more data+parity shards")

func (r HashTag) readHashTagSpec(filePath string) (nums []int, err error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil { return nil, err }

	lines := strings.Split(string(b), "\n")

	// Each code specification contains description of parity chunk computation and partitions.
	// Each parity chunk is specified by two matrices: index matrix and coefficient matrix,
	// where index matrix contains 2*r.Alpha*(r.DataShards+r.KDivR) integers
	// and coefficient matrix contains r.Alpha*(r.DataShards+r.KDivR) integers,
	// since each parity chunk depends on at most r.Alpha*(r.DataShards+r.KDivR) payload subchunks.
	// Specification contains r.KDivR partitions,
	// where each partition is given by r.Alpha integers.
	// The total number of integers in code specification is equal to numOfIntElements.
	numOfIntElements:=r.ParityShards*r.Alpha*(r.DataShards+r.KDivR)*3+r.KDivR*r.Alpha
	// Assign cap to avoid resize on every append.
	nums = make([]int, 0, numOfIntElements)
	j:=0
	for _,l := range lines {
		if  len(l) == 0 || l == "\r" { continue }
		li := strings.Split(l, " ")
		for _,a := range li {
			if len(a) == 0 { continue }
			if a == "\r" {
				fmt.Fprint(os.Stdout,"\n")
				break
			}
			w, err := strconv.Atoi(a)
			j++
			if err != nil {
				return nil, err
			}
			nums = append(nums, w)
			fmt.Fprint(os.Stdout,w," ")
		}
		if j==numOfIntElements { break }
	}

	return nums, nil
}

// New creates a new encoder and initializes it to
// the number of data shards and parity shards that
// you want to use. You can reuse this encoder.
func NewHashTagCode(dataShards, parityShards int) (HashTagCodec, error) {
	r := HashTag{
		DataShards:   dataShards,
		ParityShards: parityShards,
		Shards:       dataShards + parityShards,
	}
	if dataShards <= 0 || parityShards <= 0 {
		return nil, ErrInvShardNum
	}
	r.Alpha = 0
	for i:= range ParametersHashTag {
		if (ParametersHashTag[i][0]==uint16(r.Shards)) && (ParametersHashTag[i][1]==uint16(r.ParityShards)) {
			r.Alpha = int(ParametersHashTag[i][2])
			break
		}
	}
	if r.Alpha == 0 {
		return nil, errors.New("HashTag code with required parameters does not exist")
	}
	extension := 8
	r.KDivR = int((r.DataShards + r.ParityShards - 1) / r.ParityShards)

	//if dataShards+parityShards > 255 {
	//	return nil, ErrMaxShardNum
	//}

	filePath := fmt.Sprintf("HashTagSpecifications/Spec_r%d_alpha%d/Spec_n%d_r%d_alpha%d_m%d.txt",r.ParityShards,r.Alpha,r.Shards,r.ParityShards,r.Alpha,extension)
	nums, err := r.readHashTagSpec(filePath)
	if err != nil { panic(err) }
	fmt.Println(len(nums))

	r.ppIndexArrayP = make([][]int, r.ParityShards*r.Alpha)

	t:=0
	for c := 0; c < r.ParityShards; c++ {
		for iRow := 0; iRow < r.Alpha; iRow++ {
			r.ppIndexArrayP[iRow+c*r.Alpha] = make([]int,2*(r.DataShards+r.KDivR))
			for iCol := 0; iCol < 2*(r.DataShards+r.KDivR); iCol++ {
				r.ppIndexArrayP[iRow+c*r.Alpha][iCol] = nums[t]
				t++
			}
		}
	}

	r.ppCoefficients = make([][]byte, r.ParityShards*r.Alpha)
	for c := 0; c < r.ParityShards; c++ {
		for iRow := 0; iRow < r.Alpha; iRow++ {
			r.ppCoefficients[iRow+c*r.Alpha] = make([]byte,r.DataShards+r.KDivR)
			for iCol := 0; iCol < r.DataShards+r.KDivR; iCol++ {
				r.ppCoefficients[iRow+c*r.Alpha][iCol] = byte(nums[t])
				t++
			}
		}
	}

	r.ppPartitions = make([][]int, r.KDivR)
	for iRow := 0; iRow < r.KDivR; iRow++ {
		r.ppPartitions[iRow] = make([]int,r.Alpha)
		for iCol := 0; iCol < r.Alpha; iCol++ {
			r.ppPartitions[iRow][iCol] = nums[t]
			t++
		}
	}
	// memory for repair
	//r.FailedNodeID = -1
	//r.NumOfExpr = 0
	maxExprLength := 2 * (r.DataShards + r.KDivR + 1)
	maxExprNum := r.ParityShards*r.Alpha
	r.ppExpressionElements = make([][]int,maxExprNum)
	for i := 0; i < maxExprNum; i++ {
		r.ppExpressionElements[i] = make([]int,2 * maxExprLength)
	}
	r.ppExpressionCoefficients = make([][]byte,maxExprNum)
	for i := 0; i < maxExprNum; i++ {
		r.ppExpressionCoefficients[i] = make([]byte,maxExprLength)
	}
	r.pExpressionLength = make([]int,maxExprNum)
	return &r, err
}

// ErrTooFewShards is returned if too few shards where given to
// Encode/Verify/Reconstruct. It will also be returned from Reconstruct
// if there were too few shards to reconstruct the missing data.
//var ErrTooFewShards = errors.New("too few shards given")

// Encodes parity for a set of data shards.
// An array 'shards' containing data shards followed by parity shards.
// The number of shards must match the number given to New.
// Each shard is a byte array, and they must all be the same size.
// The parity shards will always be overwritten and the data shards
// will remain the same.
func (r HashTag) Encode(subshards [][]byte) error {
	if len(subshards) != r.Shards*r.Alpha {
		return ErrTooFewShards
	}

	err := checkShards(subshards, false)
	if err != nil {
		return err
	}

	// Do the coding
	for i:=r.DataShards;i<r.Shards;i++ {
		// Get the slice of output buffers.
		outputShard := subshards[i*r.Alpha:(i+1)*r.Alpha]
		r.ComputeParityShard(subshards[0:r.DataShards*r.Alpha], outputShard, i-r.DataShards)
	}
	//r.codeSomeShards(r.ppIndexArrayP,r.ppCoefficients, subshards[0:r.DataShards*r.Alpha], output, r.ParityShards, len(subshards[0]))
	return nil
}

// Multiplies a subset of rows from a coding matrix by a full set of
// input shards to produce some output shards.
// 'matrixRows' is The rows from the matrix to use.
// 'inputs' An array of byte arrays, each of which is one input shard.
// The number of inputs used is determined by the length of each matrix row.
// outputs Byte arrays where the computed shards are stored.
// The number of outputs computed, and the
// number of matrix rows used, is determined by
// outputCount, which is the number of outputs to compute.
/*func (r HashTag) codeSomeShards(ppIndexArrayP [][]int, ppCoefficients, inputs, outputs [][]byte, outputCount, byteCount int) {
	// Encoding for first k columns of array ppIndexArrayP
	for shardID := 0; shardID < r.DataShards; shardID++ {
		for subshardID := 0;subshardID < r.Alpha; subshardID++ {
			in := inputs[shardID*r.Alpha+subshardID]
			for parityID := 0; parityID < outputCount; parityID++ {
				if shardID == 0 {
					galMulSlice(ppCoefficients[parityID*r.Alpha+subshardID][shardID], in, outputs[parityID*r.Alpha+subshardID])
				} else {
					galMulSliceXor(ppCoefficients[parityID*r.Alpha+subshardID][shardID], in, outputs[parityID*r.Alpha+subshardID])
				}
			}
		}
	}
	// Encoding for KDivR last columns of array ppIndexArrayP
	for parityID := 0; parityID < outputCount; parityID++ {
		for ci := 0;ci < r.Alpha; ci++ {
			for i := 0;i < r.KDivR; i++ {
				subshardID := ppIndexArrayP[parityID*r.Alpha+ci][2*(r.DataShards+i)]
				shardID := ppIndexArrayP[parityID*r.Alpha+ci][2*(r.DataShards+i)+1]
				if ppCoefficients[parityID*r.Alpha+ci][r.DataShards+i] == 0 {
					continue
				}
				in := inputs[shardID*r.Alpha+subshardID]
				galMulSliceXor(ppCoefficients[parityID*r.Alpha+ci][r.DataShards+i], in, outputs[parityID*r.Alpha+ci])
			}
		}
	}
}*/

func (r HashTag) ComputeParityShard(inputs, outputShard [][]byte, parityID int) {
	/*if runtime.GOMAXPROCS(0) > 1 && len(inputs[0]) > minSplitSize {
		r.codeSomeShardsP(matrixRows, inputs, outputs, outputCount, byteCount)
		return
	}*/
	// Encoding for first k columns of array ppIndexArrayP
	for payloadID := 0; payloadID < r.DataShards; payloadID++ {
		for subID := 0;subID < r.Alpha; subID++ {
			in := inputs[payloadID*r.Alpha+subID]
			if payloadID == 0 {
				galMulSlice(r.ppCoefficients[parityID*r.Alpha+subID][payloadID], in, outputShard[subID])
			} else {
				galMulSliceXor(r.ppCoefficients[parityID*r.Alpha+subID][payloadID], in, outputShard[subID])
			}
		}
	}
	// Encoding for KDivR last columns of array ppIndexArrayP
	for paritySubID := 0;paritySubID < r.Alpha; paritySubID++ {
		for i := 0;i < r.KDivR; i++ {
			subshardID := r.ppIndexArrayP[parityID*r.Alpha+paritySubID][2*(r.DataShards+i)]
			shardID := r.ppIndexArrayP[parityID*r.Alpha+paritySubID][2*(r.DataShards+i)+1]
			if r.ppCoefficients[parityID*r.Alpha+paritySubID][r.DataShards+i] == 0 {
				continue
			}
			in := inputs[shardID*r.Alpha+subshardID]
			galMulSliceXor(r.ppCoefficients[parityID*r.Alpha+paritySubID][r.DataShards+i], in, outputShard[paritySubID])
		}
	}
}

// Split a data slice into the number of subshards given to the encoder,
// and create empty parity subshards.
//
// The data will be split into equally sized shards.
// If the data size isn't divisible by the number of shards,
// the last shard will contain extra zeros.
//
// There must be at least 1 byte otherwise ErrShortData will be
// returned.
//
// The data will not be copied, except for the last shard, so you
// should not modify the data of the input slice afterwards.
func (r HashTag) Split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, ErrShortData
	}
	// Calculate number of bytes per shard.
	perSubshard := (len(data) + r.DataShards*r.Alpha - 1) / (r.DataShards*r.Alpha)

	// Pad data to r.Shards*r.Alpha*perShard.
	padding := make([]byte, (r.Shards*r.Alpha*perSubshard)-len(data))
	data = append(data, padding...)

	// Split into equal-length shards.
	dst := make([][]byte, r.Shards*r.Alpha)
	for i := range dst {
		dst[i] = data[:perSubshard]
		data = data[perSubshard:]
	}

	return dst, nil
}

// Join the shards and write the data segment to dst.
//
// Only the data shards are considered.
// You must supply the exact output size you want.
// If there are to few shards given, ErrTooFewShards will be returned.
// If the total data size is less than outSize, ErrShortData will be returned.
func (r HashTag) Join(dst io.Writer, shards [][]byte, outSize int) error {
	// Do we have enough shards?
	if len(shards) < r.DataShards*r.Alpha {
		return ErrTooFewShards
	}
	shards = shards[:r.DataShards*r.Alpha]

	// Do we have enough data?
	size := 0
	for _, shard := range shards {
		size += len(shard)
	}
	if size < outSize {
		return ErrShortData
	}

	// Copy data to dst
	write := outSize
	for _, shard := range shards {
		if write < len(shard) {
			_, err := dst.Write(shard[:write])
			return err
		}
		n, err := dst.Write(shard)
		if err != nil {
			return err
		}
		write -= n
	}
	return nil
}

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm;
// otherwise WriteFile truncates it before writing.
func WriteSubshardsIntoFile(filename string, data [][]byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	for i:=0; i<len(data); i++ {
		f.Write(data[i])
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func WriteShard(fname string, nodeID int, shard [][]byte) error {
	outfn := fmt.Sprintf("%s.%d", fname, nodeID)
	fmt.Println("Writing to ", outfn)
	err := WriteSubshardsIntoFile(outfn,shard,os.ModePerm)
	return err
}

var ParametersHashTag = [41][4]uint16{
	[4]uint16{ 4, 2,  4, 8},
	[4]uint16{ 5, 2,  4, 8},
	[4]uint16{ 6, 2,  4, 8},
	[4]uint16{ 7, 2,  8, 8},
	[4]uint16{ 8, 2,  8, 8},
	[4]uint16{ 9, 2,  8, 8},
	[4]uint16{10, 2,  8, 8},
	[4]uint16{11, 2,  8, 8},
	[4]uint16{12, 2,  8, 8},
	[4]uint16{13, 2,  8, 8},
	[4]uint16{14, 2,  8, 8},
	[4]uint16{15, 2,  8, 8},
	[4]uint16{16, 2,  8, 8},
	[4]uint16{17, 2,  8, 8},
	[4]uint16{18, 2,  8, 8},
	[4]uint16{ 6, 3,  9, 8},
	[4]uint16{ 7, 3,  9, 8},
	[4]uint16{ 8, 3,  9, 8},
	[4]uint16{ 9, 3,  9, 8},
	[4]uint16{10, 3,  9, 8},
	[4]uint16{11, 3,  9, 8},
	[4]uint16{12, 3,  9, 8},
	[4]uint16{13, 3,  9, 8},
	[4]uint16{14, 3,  9, 8},
	[4]uint16{15, 3,  9, 8},
	[4]uint16{16, 3,  9, 8},
	[4]uint16{17, 3,  9, 8},
	[4]uint16{18, 3,  9, 8},
	[4]uint16{ 8, 4, 16, 8},
	[4]uint16{ 9, 4, 16, 8},
	[4]uint16{10, 4, 16, 8},
	[4]uint16{11, 4, 16, 8},
	[4]uint16{12, 4, 16, 8},
	[4]uint16{13, 4, 16, 8},
	[4]uint16{14, 4, 16, 8},
	[4]uint16{15, 4, 16, 8},
	[4]uint16{16, 4, 16, 8},
	[4]uint16{10, 5, 10, 8},
	[4]uint16{11, 5, 10, 8},
	[4]uint16{12, 5, 10, 8},
	[4]uint16{13, 5, 10, 8}}





