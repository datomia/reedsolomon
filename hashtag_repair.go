package reedsolomon

import (
	"fmt"
	"errors"
	"os"
)

/*func AllocateMemoryForRepair(r HashTag) {
	maxExprLength := 2 * (r.DataShards + r.k_div_r + 1)
	maxExprNum := r.ParityShards*r.Alpha
	r.ppExpressionElements = make([][]int,maxExprNum)
	for i := 0; i < maxExprNum; i++ {
		r.ppExpressionElements[i] = make([]int,2 * maxExprLength)
	}
	r.ppExpressionCoefficients = make([][]byte,maxExprNum)
	for i := 1; i < maxExprNum; i++ {
		r.ppExpressionCoefficients[i] = make([]byte,maxExprLength)
	}
	r.pExpressionLength = make([]int,maxExprNum)
}*/

// galDivide is inverse of galMultiply.
func galDivideArray(num int, pArray []byte, divisor byte) {
	for i:=0; i<num; i++ {
		pArray[i] = galDivide(pArray[i],divisor)
	}
}

/* Generate expression for rapair.
Output expression is given by pExprElements and pExprCoeffs. */
func (r HashTag) GenExpression(
unknownVarExprPos, checkNodeID, checkRow, num int, // number of coefficients
pInputElements []int, pInputCoeffs []byte,
pExprElements []int, pExprCoeffs []byte) int {
	/* Expression for parity node computation is employed to obtain expression for unknown variable computation,
	more precisely, we divide equation for parity node by coefficient for unknown variable and then move
	parity nodes to other summands, while unknown variable is moved to empty side of the equation. */
	copy(pExprCoeffs[0:num], pInputCoeffs[0:num]); // last m_k_div_r elements are equal to 0
	// Obtain coefficient=1 for a_{i,l}
	// Assumption: pTemp[failedNodeID]>0
	divisor := pExprCoeffs[unknownVarExprPos]
	if divisor == 0 {
		errors.New("Division by 0 in function GenExpression")
		return 0
	}
	galDivideArray(num, pExprCoeffs, divisor)
	pExprCoeffs[unknownVarExprPos] = galDivide(1, divisor)

	// Initialize expression elements
	copy(pExprElements[0:2*num], pInputElements[0:2*num]) // last m_k_div_r elements are equal to 0
	// Variable to be computed is replaced by parity node
	pExprElements[2*unknownVarExprPos] = checkRow
	pExprElements[2*unknownVarExprPos + 1] = checkNodeID
	// Exclude from the equation all variables with 0 coefficients
	for i := 0; i < num; i++ {
		if (pExprCoeffs[i] == 0) {
			pExprCoeffs[i] = pExprCoeffs[num - 1]
			pExprElements[2 * i] = pExprElements[2 * (num - 1)]
			pExprElements[2 * i+1] = pExprElements[2 * (num - 1)+1]
			num--
			i--
		}
	}
	return num // return length of the expression
}

/* Construct expressions for reconstruction of alpha subchunks of failedNodeID-th storage node.
Assumption: only one storage node is failed.
Implementation of the Algorithm 4.
Return
1. The number of expressions
2. Lengths of expressions
3. Elements participating in expressions
4. Coefficients for the expressions
where i-th expression is specified by
its length len=pExpressionLength[i],
elements ppExpressionElements[i][0],...,ppExpressionElements[i][2*len-1],
coefficients ppExpressionCoefficients[i][0],...,ppExpressionCoefficients[i][len-1],
and element to be computed (ppExpressionElements[i][2*len],ppExpressionElements[i][2*len+1])
pTemp: length at least k.
Return 0 in case of failure. */
func (r HashTag) ExpressionsForRepairSingleSystematicNode(failedNodeID int) (int,error) {
	/*if r.pExpressionLength == nil {
		r.AllocateMemoryForRepair()
	}*/
	/* Line 1 of Algorithm 4: Access and transfer (k-1)*ceil(alpha/r) elements a_{i,j}
	from all (k-1) non-failed systematic nodes
	and ceil(alpha/r) elements p_{i,0} from p_0, where i\in D_{rho,d_l}. Here l=failedNodeID.*/
	numOfExpr := 0
	nu := failedNodeID / r.ParityShards // index of the set containing failed node
	elementID := failedNodeID%r.ParityShards
	portion := int((r.Alpha + r.ParityShards - 1) / r.ParityShards)
	pSubset := r.ppPartitions[nu][elementID*portion:]
	maxExprLength := 2 * (r.DataShards + r.k_div_r + 1)
	/*Line 2.
	Operations to repair a_{i,j}, where i\in D_{rho,d_l}.
	List of elements to be computed.
	Expressions for elements to be computed, where expression is given by a list of utilized elements and coefficients,
	where an element is given by pair (row index, column index)*/
	for i := 0; i < portion; i++ {
		id := pSubset[i]
		// Expression for a_{i,l}, where i\in D_{rho,d_l}
		r.pExpressionLength[numOfExpr] = r.GenExpression(failedNodeID, r.DataShards, id, r.DataShards,
			r.ppIndexArrayP[id], r.ppCoefficients[id],
			r.ppExpressionElements[numOfExpr], r.ppExpressionCoefficients[numOfExpr])
		if (r.pExpressionLength[numOfExpr] > maxExprLength) {
			return 0,errors.New("Error in function ExpressionsForRepairSingleSystematicNode: maxExprLength is too low.")
		}

		e := r.pExpressionLength[numOfExpr]
		// Element to be computed
		r.ppExpressionElements[numOfExpr][2 * e] = pSubset[i]
		r.ppExpressionElements[numOfExpr][2 * e + 1] = failedNodeID
		numOfExpr++
	}
	/* Line 3.
	Access and transfer (r-1)*ceil(alpha/r) elements p_{i,j} from p_1,...,p_{r-1},
	where i\in D_{rho,d_l}*/
	/*Line 4.
	Access and transfer from the systematic nodes the elements a_{i,j} listed
	in the i-th row of the arrays P_1,...,P_{r-1},
	where i\in D_{rho,d_l}, that have not been read in Step 1*/
	// length of pTempBool is at least alpha
	pTempBool := make([]bool,r.Alpha)
	//memset(pTempBool, 0, r.Alpha)
	for i := 0; i < portion; i++ {
		pTempBool[pSubset[i]] = true
	}
	for i := 1; i < r.ParityShards; i++ {
		for j := 0; j < portion; j++ {
			// There is only one element (i,failedNodeID) in each expression,
			// where i\in D\D_{rho,d_l}
			// Check this for each expression. Save unknown element.
			unknownNum := 0
			unknownVarExprPos := 0
			for t := r.DataShards; t < r.DataShards + r.k_div_r; t++ {
			// we start from k, since all previous elements are assumed to be already computed
				if r.ppIndexArrayP[i*r.Alpha + pSubset[j]][2 * t + 1] != failedNodeID {
					continue
				}
				if pTempBool[r.ppIndexArrayP[i*r.Alpha + pSubset[j]][2 * t]] {
					continue
				}
				unknownVarExprPos = t
				unknownNum++
			}
			if unknownNum != 1 {
				return 0,errors.New("Error: number of unknown variables is not equal to one!")
			}
			unknownVarRow := r.ppIndexArrayP[i*r.Alpha + pSubset[j]][2 * unknownVarExprPos]
			r.pExpressionLength[numOfExpr] = r.GenExpression(unknownVarExprPos, r.DataShards+i, pSubset[j],
			r.DataShards + r.k_div_r, r.ppIndexArrayP[i*r.Alpha + pSubset[j]], r.ppCoefficients[i*r.Alpha + pSubset[j]],
				r.ppExpressionElements[numOfExpr], r.ppExpressionCoefficients[numOfExpr]);
			if (r.pExpressionLength[numOfExpr] > maxExprLength) {
				return 0,errors.New("Error: m_maxExprLength is too low.")
			}
			e := r.pExpressionLength[numOfExpr]
			r.ppExpressionElements[numOfExpr][2 * e] = unknownVarRow
			r.ppExpressionElements[numOfExpr][2 * e + 1] = failedNodeID

			numOfExpr++
		}
	}
	maxNumOfExpressions := r.ParityShards*r.Alpha
	if numOfExpr > maxNumOfExpressions {
		return 0,errors.New("Error: maxNumOfExpressions is too low.")
	}
	return numOfExpr,nil
}

/* Returns the total number of elements to be read for decoding,
ppToRead indicates elements to be read. */
func ElementsToRead(n, alpha, numOfExpr int,
	pExpressionLength []int,
	ppExpressionElements [][]int) ([]bool) {
	pToRead := make([]bool,alpha*n)
	for i := 0; i < numOfExpr; i++ {
		for j := 0; j < pExpressionLength[i]; j++ {
			row := ppExpressionElements[i][2 * j]
			column := ppExpressionElements[i][2 * j + 1]
			pToRead[row*n+column] = true
		}
	}
	for i := 0; i < numOfExpr; i++ {
		row := ppExpressionElements[i][2 * pExpressionLength[i]]
		column := ppExpressionElements[i][2 * pExpressionLength[i] + 1]
		pToRead[row*n+column] = false
	}
	/*numOfElementsToRead := 0
	for i := 0; i < alpha*n; i++ {
		if pToRead[i] {
			numOfElementsToRead++
		}
	}*/
	return pToRead
}

func (r HashTag) Repair(fname string, pIfFailedSN []bool, subshardSize int64, shards [][]byte) error {
	numOfFailures :=0
	failedNodeID:=0
	// compute number of failed storage nodes and store identifier of the last failed node
	for i := 0; i < r.Shards; i++ {
		if (pIfFailedSN[i]) {
			numOfFailures++
			failedNodeID = i
		}
	}
	switch numOfFailures {
	case 0:
		// nothing to do
		fmt.Println("Nothing to repair.\n")
		break
	case 1:
		// check whether systematic or non-systematic storage node has failed
		if failedNodeID < r.DataShards {
			err := r.RepairSystematicStorageNode(fname, failedNodeID, subshardSize, shards)
			return err
		} else {
			for i:=0;i<r.Shards*r.Alpha;i++ {
				shards[i] = make([]byte, subshardSize)
			}
			// read payload data
			decodingIsNeeded, _, _, err := r.ReadShards(fname, subshardSize, shards)
			if err != nil { return err }
			if decodingIsNeeded { return errors.New("Failed to read payload data for repair of parity storage node")}
			r.ComputeParityShard(shards[0:r.DataShards*r.Alpha], shards[failedNodeID*r.Alpha:(failedNodeID+1)*r.Alpha], failedNodeID-r.DataShards)
			// write reconstructed parity chunk
			err = WriteShard(fname, failedNodeID, shards[failedNodeID*r.Alpha:(failedNodeID+1)*r.Alpha])
			return nil //errors.New("Repair for failure of parity storage nodes is not implemented.")
		}
	default:
		if numOfFailures > r.ParityShards {
			fmt.Println("Data reconstruction is impossible.")
			fmt.Printf("Only %d storage nodes are available, but at least %d storage nodes are required.",r.Shards-numOfFailures,r.DataShards)
			return nil
		}
		return errors.New("Repair for several storage node failures is not implemented.")
	}
	return nil
}

/* Compute one subchunk to repair a single storage node failure.
Result is stored in pSubchunk. */
func ComputeSubchunkAccordingExpression(numOfSubchunksInChunk, len int,
pElements []int, pCoefficients []byte,
pValues [][]byte, pSubchunk []byte) {
	for j := 0; j < len; j++ {
		id := pElements[2 * j + 1] * numOfSubchunksInChunk + pElements[2 * j]
		if j == 0 {
			galMulSlice(pCoefficients[j], pValues[id], pSubchunk)
		} else {
			galMulSliceXor(pCoefficients[j], pValues[id], pSubchunk)
		}
	}
}

/* Compute subchunks to repair a single storage node failure.
Result is stored in pValues. */
func (r *HashTag) ComputeAccordingExpressions(numOfExpr int, ppValues [][]byte) {
	for i := 0; i < numOfExpr; i++ {
		exLen := r.pExpressionLength[i]
		subchunkID := r.ppExpressionElements[i][2 * exLen]
		SN_ID := r.ppExpressionElements[i][2 * exLen + 1]
		id := SN_ID*r.Alpha + subchunkID
		ComputeSubchunkAccordingExpression(r.Alpha,
			exLen, r.ppExpressionElements[i], r.ppExpressionCoefficients[i],
		ppValues, ppValues[id]);
	}
}

/* Reconstruct data stored on failed storage nodes. */
func (r HashTag) RepairSystematicStorageNode(fname string, failedNodeID int, subshardSize int64, shards [][]byte) error {
	// construct expressions for data reconstruction

	// construct expressions for repair
	//if (r.FailedNodeID != failedNodeID) {
	numOfExpr,err := r.ExpressionsForRepairSingleSystematicNode(failedNodeID)
	if err != nil {
		return err
	}
	//}
	// identify elements to be transferred from storage nodes
	pToRead := ElementsToRead(r.Shards,
		r.Alpha, numOfExpr,
		r.pExpressionLength, r.ppExpressionElements)

	for i:=0;i<r.Shards*r.Alpha;i++ {
		shards[i] = make([]byte, subshardSize)
	}
	// read shards
	// case of systemtic storage node failure
	// download data according to pToRead
	// Create shards and load necessary data.
	for i:=0;i<r.Shards;i++ {
		if i == failedNodeID {
			continue
		}
		infn := fmt.Sprintf("%s.%d", fname, i)
		fmt.Println("Opening", infn)
		f, err := os.Open(infn)
		if err != nil {
			return errors.New(fmt.Sprintf("Error reading file %s",infn))
		}
		for j:=0;j<r.Alpha;j++ {
			if pToRead[j*r.Shards+i] == false {
				continue
			}
			// whence: 0 means relative to the origin of the file, 1 means relative to the current offset, and 2 means relative to the end
			_, err := f.Seek(int64(j)*subshardSize, 0)
			if err != nil {
				return errors.New(fmt.Sprintf("Error seeking position %d in file %s",int64(j)*subshardSize,infn))
			}
			fmt.Printf("Reading %d-th subchunk\n",j)
			_, err = f.Read(shards[i*r.Alpha+j])
			if err != nil {
				return errors.New(fmt.Sprintf("Error reading %d-th subshard in file %s",j,infn))
			}
			//fmt.Printf("%d bytes @ %d: %s\n", n2, o2, string(shards[i*r.Alpha+j]))
		}
		f.Close()
	}
	fmt.Printf("Data transferred from storage nodes for repair of %d-th storage node\n",failedNodeID)
	fmt.Println("Here one column corresponds to one storage node")
	num:=0
	for i:=0;i<r.Shards*r.Alpha;i++ {
		if pToRead[i] {
			fmt.Printf("1 ")
			num++
		} else {
			fmt.Printf("0 ")
		}
		if (i+1)%r.Shards == 0 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf("Number of transferred subchunks: %d\n",num)
	fmt.Printf("In case of Reed-Solomon codes: %d\n",r.DataShards*r.Alpha)
	// compute data for systematic storage node according to expressions
	// elements should be reconstructed in specified order (according to expression's ID)
	r.ComputeAccordingExpressions(numOfExpr, shards)

	// write reconstructed subshards to new file
	err = WriteShard(fname, failedNodeID, shards[failedNodeID*r.Alpha:(failedNodeID+1)*r.Alpha])
	return err
}

