/**
 * Reed-Solomon Coding over 8-bit values.
 *
 * Copyright 2015, Klaus Post
 * Copyright 2015, Backblaze, Inc.
 */

// Package reedsolomon enables Erasure Coding in Go
//
// For usage and examples, see https://github.com/klauspost/reedsolomon
//
package reedsolomon

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
)

// Encoder is an interface to encode Reed-Salomon parity sets for your data.
type Encoder interface {
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
	Verify(shards [][]byte) (bool, error)

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
	Reconstruct(shards [][]byte, idxs ...int) error

	// Split a data slice into the number of shards given to the encoder,
	// and create empty parity shards.
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
}

// reedSolomon contains a matrix for a specific
// distribution of datashards and parity shards.
// Construct if using New()
type reedSolomon struct {
	DataShards   int // Number of data shards, should not be modified.
	ParityShards int // Number of parity shards, should not be modified.
	Shards       int // Total number of shards. Calculated, and should not be modified.
	m            matrix
	parity       [][]byte
}

// ErrInvShardNum will be returned by New, if you attempt to create
// an Encoder where either data or parity shards is zero or less.
var ErrInvShardNum = errors.New("cannot create Encoder with zero or less data/parity shards")

// ErrMaxShardNum will be returned by New, if you attempt to create
// an Encoder where data and parity shards cannot be bigger than
// Galois field GF(2^8) - 1.
var ErrMaxShardNum = errors.New("cannot create Encoder with 255 or more data+parity shards")

// New creates a new encoder and initializes it to
// the number of data shards and parity shards that
// you want to use. You can reuse this encoder.
// Note that the maximum number of data shards is 256.
func New(dataShards, parityShards int) (Encoder, error) {
	r := reedSolomon{
		DataShards:   dataShards,
		ParityShards: parityShards,
		Shards:       dataShards + parityShards,
	}

	if dataShards <= 0 || parityShards <= 0 {
		return nil, ErrInvShardNum
	}

	if dataShards+parityShards > 255 {
		return nil, ErrMaxShardNum
	}

	// Start with a Vandermonde matrix.  This matrix would work,
	// in theory, but doesn't have the property that the data
	// shards are unchanged after encoding.
	vm, err := vandermonde(r.Shards, dataShards)
	if err != nil {
		return nil, err
	}

	// Multiply by the inverse of the top square of the matrix.
	// This will make the top square be the identity matrix, but
	// preserve the property that any square subset of rows  is
	// invertible.
	top, _ := vm.SubMatrix(0, 0, dataShards, dataShards)
	top, _ = top.Invert()
	r.m, _ = vm.Multiply(top)

	r.parity = make([][]byte, parityShards)
	for i := range r.parity {
		r.parity[i] = r.m[dataShards+i]
	}

	return &r, err
}

// ErrTooFewShards is returned if too few shards where given to
// Encode/Verify/Reconstruct. It will also be returned from Reconstruct
// if there were too few shards to reconstruct the missing data.
var ErrTooFewShards = errors.New("too few shards given")

// Encodes parity for a set of data shards.
// An array 'shards' containing data shards followed by parity shards.
// The number of shards must match the number given to New.
// Each shard is a byte array, and they must all be the same size.
// The parity shards will always be overwritten and the data shards
// will remain the same.
func (r reedSolomon) Encode(shards [][]byte) error {
	if len(shards) != r.Shards {
		return ErrTooFewShards
	}

	err := checkShards(shards, false)
	if err != nil {
		return err
	}

	// Get the slice of output buffers.
	output := shards[r.DataShards:]

	// Do the coding.
	r.codeSomeShards(r.parity, shards[0:r.DataShards], output, r.ParityShards, len(shards[0]))
	return nil
}

// Verify returns true if the parity shards contain the right data.
// The data is the same format as Encode. No data is modified.
func (r reedSolomon) Verify(shards [][]byte) (bool, error) {
	if len(shards) != r.Shards {
		return false, ErrTooFewShards
	}
	err := checkShards(shards, false)
	if err != nil {
		return false, err
	}

	// Slice of buffers being checked.
	toCheck := shards[r.DataShards:]

	// Do the checking.
	return r.checkSomeShards(r.parity, shards[0:r.DataShards], toCheck, r.ParityShards, len(shards[0])), nil
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
func (r reedSolomon) codeSomeShards(matrixRows, inputs, outputs [][]byte, outputCount, byteCount int) {
	if runtime.GOMAXPROCS(0) > 1 && len(inputs[0]) > minSplitSize {
		r.codeSomeShardsP(matrixRows, inputs, outputs, outputCount, byteCount)
		return
	}
	for c := 0; c < r.DataShards; c++ {
		in := inputs[c]
		for iRow := 0; iRow < outputCount; iRow++ {
			if c == 0 {
				galMulSlice(matrixRows[iRow][c], in, outputs[iRow])
			} else {
				galMulSliceXor(matrixRows[iRow][c], in, outputs[iRow])
			}
		}
	}
}

const (
	minSplitSize  = 512 // min split size per goroutine
	maxGoroutines = 50  // max goroutines number for encoding & decoding
)

// Perform the same as codeSomeShards, but split the workload into
// several goroutines.
func (r reedSolomon) codeSomeShardsP(matrixRows, inputs, outputs [][]byte, outputCount, byteCount int) {
	var wg sync.WaitGroup
	do := byteCount / maxGoroutines
	if do < minSplitSize {
		do = minSplitSize
	}
	start := 0
	for start < byteCount {
		if start+do > byteCount {
			do = byteCount - start
		}
		wg.Add(1)
		go func(start, stop int) {
			for c := 0; c < r.DataShards; c++ {
				in := inputs[c]
				for iRow := 0; iRow < outputCount; iRow++ {
					if c == 0 {
						galMulSlice(matrixRows[iRow][c], in[start:stop], outputs[iRow][start:stop])
					} else {
						galMulSliceXor(matrixRows[iRow][c], in[start:stop], outputs[iRow][start:stop])
					}
				}
			}
			wg.Done()
		}(start, start+do)
		start += do
	}
	wg.Wait()
}

// checkSomeShards is mostly the same as codeSomeShards,
// except this will check values and return
// as soon as a difference is found.
func (r reedSolomon) checkSomeShards(matrixRows, inputs, toCheck [][]byte, outputCount, byteCount int) bool {
	same := true
	var mu sync.RWMutex // For above

	var wg sync.WaitGroup
	do := byteCount / maxGoroutines
	if do < minSplitSize {
		do = minSplitSize
	}
	start := 0
	for start < byteCount {
		if start+do > byteCount {
			do = byteCount - start
		}
		wg.Add(1)
		go func(start, do int) {
			defer wg.Done()
			outputs := make([][]byte, len(toCheck))
			for i := range outputs {
				outputs[i] = make([]byte, do)
			}
			for c := 0; c < r.DataShards; c++ {
				mu.RLock()
				if !same {
					mu.RUnlock()
					return
				}
				mu.RUnlock()
				in := inputs[c][start : start+do]
				for iRow := 0; iRow < outputCount; iRow++ {
					galMulSliceXor(matrixRows[iRow][c], in, outputs[iRow])
				}
			}

			for i, calc := range outputs {
				if !bytes.Equal(calc, toCheck[i][start:start+do]) {
					mu.Lock()
					same = false
					mu.Unlock()
					return
				}
			}
		}(start, do)
		start += do
	}
	wg.Wait()
	return same
}

// ErrShardNoData will be returned if there are no shards,
// or if the length of all shards is zero.
var ErrShardNoData = errors.New("no shard data")

// ErrShardSize is returned if shard length isn't the same for all
// shards.
var ErrShardSize = errors.New("shard sizes does not match")

// checkShards will check if shards are the same size
// or 0, if allowed. An error is returned if this fails.
// An error is also returned if all shards are size 0.
func checkShards(shards [][]byte, nilok bool) error {
	size := shardSize(shards)
	if size == 0 {
		return ErrShardNoData
	}
	for _, shard := range shards {
		if len(shard) != size {
			if len(shard) != 0 || !nilok {
				return ErrShardSize
			}
		}
	}
	return nil
}

// shardSize return the size of a single shard.
// The first non-zero size is returned,
// or 0 if all shards are size 0.
func shardSize(shards [][]byte) int {
	for _, shard := range shards {
		if len(shard) != 0 {
			return len(shard)
		}
	}
	return 0
}

// Reconstruct will recreate the missing shards, if possible.
//
// Given a list of shards, some of which contain data, fills in the
// ones that don't have data.
//
// The length of the array must be equal to Shards.
// You indicate that a shard is missing by setting it to nil.
//
// If there are too few shards to reconstruct the missing
// ones, ErrTooFewShards will be returned.
//
// The reconstructed shard set is complete, but integrity is not verified.
// Use the Verify function to check if data set is ok.
func (r reedSolomon) Reconstruct(shards [][]byte, idxs ...int) error {
	return r.reconstruct(shards, idxs...)
}

func (r reedSolomon) reconstruct(shards [][]byte, idxs ...int) error {
	if len(shards) != r.Shards {
		return ErrTooFewShards
	}
	// Check that all indexes are in correct range.
	for _, idx := range idxs {
		if idx < 0 || idx >= len(shards) {
			return fmt.Errorf("reconstruct is not allowed. requested index is out of range. %v", idx)
		}
	}

	// Check arguments.
	err := checkShards(shards, true)
	if err != nil {
		return err
	}

	shardSize := shardSize(shards)

	// Quick check: are all of the shards present?  If so, there's
	// nothing to do.
	numberPresent := 0
	requiredPresent := 0
	for i := 0; i < r.Shards; i++ {
		if len(shards[i]) != 0 {
			if len(idxs) > 0 && contains(idxs, i) {
				requiredPresent++
			}
			numberPresent++
		}
	}
	if numberPresent == r.Shards || (len(idxs) > 0 && len(idxs) == requiredPresent) {
		// Cool.  All of the shards data data.  We don't
		// need to do anything.
		return nil
	}

	// More complete sanity check
	if numberPresent < r.DataShards {
		return ErrTooFewShards
	}

	// Check if any of requested index is in parity range. In that case we will need to reconstruct all data shards.
	var needAllData bool
	for _, idx := range idxs {
		if idx >= r.DataShards {
			needAllData = true
			break
		}
	}

	// Pull out the rows of the matrix that correspond to the
	// shards that we have and build a square matrix.  This
	// matrix could be used to generate the shards that we have
	// from the original data.
	//
	// Also, pull out an array holding just the shards that
	// correspond to the rows of the submatrix.  These shards
	// will be the input to the decoding process that re-creates
	// the missing data shards.
	subMatrix, _ := newMatrix(r.DataShards, r.DataShards)
	subShards := make([][]byte, r.DataShards)
	subMatrixRow := 0
	for matrixRow := 0; matrixRow < r.Shards && subMatrixRow < r.DataShards; matrixRow++ {
		if len(shards[matrixRow]) != 0 {
			for c := 0; c < r.DataShards; c++ {
				subMatrix[subMatrixRow][c] = r.m[matrixRow][c]
			}
			subShards[subMatrixRow] = shards[matrixRow]
			subMatrixRow++
		}
	}

	// Invert the matrix, so we can go from the encoded shards
	// back to the original data.  Then pull out the row that
	// generates the shard that we want to decode.  Note that
	// since this matrix maps back to the original data, it can
	// be used to create a data shard, but not a parity shard.
	dataDecodeMatrix, err := subMatrix.Invert()
	if err != nil {
		return err
	}

	// Re-create any data shards that were missing.
	//
	// The input to the coding is all of the shards we actually
	// have, and the output is the missing data shards.  The computation
	// is done using the special decode matrix we just built.
	outputs := make([][]byte, r.ParityShards)
	matrixRows := make([][]byte, r.ParityShards)
	outputCount := 0

	for iShard := 0; iShard < r.DataShards; iShard++ {
		if len(shards[iShard]) == 0 {
			if !needAllData && len(idxs) > 0 && !contains(idxs, iShard) {
				continue
			}
			shards[iShard] = make([]byte, shardSize)
			outputs[outputCount] = shards[iShard]
			matrixRows[outputCount] = dataDecodeMatrix[iShard]
			outputCount++
		}
	}
	r.codeSomeShards(matrixRows, subShards, outputs[:outputCount], outputCount, shardSize)
	// Now that we have all of the data shards intact, we can
	// compute any of the parity that is missing.
	//
	// The input to the coding is ALL of the data shards, including
	// any that we just calculated.  The output is whichever of the
	// data shards were missing.
	outputCount = 0
	for iShard := r.DataShards; iShard < r.Shards; iShard++ {
		if len(shards[iShard]) == 0 {
			if len(idxs) > 0 && !contains(idxs, iShard) {
				continue
			}
			shards[iShard] = make([]byte, shardSize)
			outputs[outputCount] = shards[iShard]
			matrixRows[outputCount] = r.parity[iShard-r.DataShards]
			outputCount++
		}
	}
	r.codeSomeShards(matrixRows, shards[:r.DataShards], outputs[:outputCount], outputCount, shardSize)
	return nil
}

func contains(src []int, i int) bool {
	for _, v := range src {
		if v == i {
			return true
		}
	}
	return false
}

// ErrShortData will be returned by Split(), if there isn't enough data
// to fill the number of shards.
var ErrShortData = errors.New("not enough data to fill the number of requested shards")

// Split a data slice into the number of shards given to the encoder,
// and create empty parity shards.
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
func (r reedSolomon) Split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, ErrShortData
	}
	// Calculate number of bytes per shard.
	perShard := (len(data) + r.DataShards - 1) / r.DataShards

	// Pad data to r.Shards*perShard.
	padding := make([]byte, (r.Shards*perShard)-len(data))
	data = append(data, padding...)

	// Split into equal-length shards.
	dst := make([][]byte, r.Shards)
	for i := range dst {
		dst[i] = data[:perShard]
		data = data[perShard:]
	}

	return dst, nil
}

// Join the shards and write the data segment to dst.
//
// Only the data shards are considered.
// You must supply the exact output size you want.
// If there are to few shards given, ErrTooFewShards will be returned.
// If the total data size is less than outSize, ErrShortData will be returned.
func (r reedSolomon) Join(dst io.Writer, shards [][]byte, outSize int) error {
	// Do we have enough shards?
	if len(shards) < r.DataShards {
		return ErrTooFewShards
	}
	shards = shards[:r.DataShards]

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
