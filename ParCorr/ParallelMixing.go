package main

/*
	1. window size must be even
*/

import (
	"fmt"
	"log"
	"runtime"
	"sort"
)

// pair for storing a subvector of a sketch
type SubvectorProfile struct {
	id            int
	first, second float64
}

// pair structure for ID pairs of two time series
type IDPair struct {
	first, second int
}

func (p IDPair) String() string {
	return fmt.Sprintf("(%v, %v)", p.first, p.second)
}

// search all the highly correlated pairs based on computed sketch representations
func ParallelMixing(tsProfiles []TimeSeriesProfile) []IDPair {
	timeSeriesNum := len(tsProfiles)
	batchNum := runtime.NumCPU()
	batchSize := timeSeriesNum / batchNum

	// one channel for the subvectors at the same position (which is sketchSize/2)
	subvectorsProfilesChans := make([]chan []SubvectorProfile, sketchSize/2)
	// for each channel, its buffer size is number of all time series
	for i := 0; i < len(subvectorsProfilesChans); i++ {
		subvectorsProfilesChans[i] = make(chan []SubvectorProfile, timeSeriesNum)
	}

	// partition sketches and send subvectors to corresponding channels
	for i := 0; i < batchNum; i++ {
		start, end := batchSize*i, batchSize*(i+1)
		if i == batchNum-1 {
			end = timeSeriesNum
		}
		go partition(tsProfiles[start:end], subvectorsProfilesChans)
	}

	// one channel for each time series to return a cluster of candidate ids
	idClusterChans := make([]chan []int, timeSeriesNum)
	// TODO:
	for i := 0; i < len(idClusterChans); i++ {
		idClusterChans[i] = make(chan []int, sketchSize/2)
	}

	for i := 0; i < sketchSize/2; i++ {
		go mapping(subvectorsProfilesChans[i], batchNum, idClusterChans)
	}

	// one channel for one time series to return highly correlated pairs (a list of idPairs)
	candidatesPairChans := make([]chan []IDPair, timeSeriesNum)
	for i := 0; i < len(candidatesPairChans); i++ {
		candidatesPairChans[i] = make(chan []IDPair, 1)
	}

	for i := 0; i < timeSeriesNum; i++ {
		go counter(i, idClusterChans[i], candidatesPairChans[i])
	}

	// Receive all the candidate correlated pairs
	pairs := []IDPair{}
	candidatesNum := 0
	for i := 0; i < timeSeriesNum; i++ {
		tsPair := <-candidatesPairChans[i]
		// Emit all pairs of candidates
		for j := 0; j < len(tsPair); j++ {
			candidatesNum++
			ts1, ts2 := tsPair[j].first, tsPair[j].second
			pearson := computePearson(tsProfiles[ts1], tsProfiles[ts2])
			if pearson >= pearsonThr {
				pairs = append(pairs, IDPair{ts1, ts2})
				// log.Println(ts1, ts2, pearson)
			}
		}
	}
	log.Println("Number of candidate pairs found: ", candidatesNum)
	log.Println("Number of pairs found: ", len(pairs))

	return pairs
}

// partition the sketches of a batch of time series
func partition(tsProfiles []TimeSeriesProfile, c []chan []SubvectorProfile) {
	subvectorProfiles := make([][]SubvectorProfile, sketchSize/2)
	// partition
	for i := 0; i < len(tsProfiles); i++ {
		for j := 0; j < sketchSize/2; j++ {
			subvectorProfiles[j] = append(subvectorProfiles[j], SubvectorProfile{tsProfiles[i].id, tsProfiles[i].sketch[j*2], tsProfiles[i].sketch[j*2+1]})
		}
	}

	// send subvectors to corresponding channels
	for i := 0; i < sketchSize/2; i++ {
		c[i] <- subvectorProfiles[i]
	}
}

// map same subvectors at the same position to the same cells in the grid
func mapping(subvectorProfilesChan <-chan []SubvectorProfile, batchNum int, idClusterChans []chan []int) {
	type subvectorID struct {
		first, second int
	}
	subvectorToTSs := make(map[subvectorID][]int)

	for i := 0; i < batchNum; i++ {
		subvectorProfiles := <-subvectorProfilesChan
		for j := 0; j < len(subvectorProfiles); j++ {
			key := subvectorID{int(subvectorProfiles[j].first) / gridSize, int(subvectorProfiles[j].second) / gridSize}
			subvectorToTSs[key] = append(subvectorToTSs[key], subvectorProfiles[j].id)
		}

	}

	// TODO: no need to sort actually, but actual runtime difference is uncertain
	for key := range subvectorToTSs {
		sort.Ints(subvectorToTSs[key])
	}

	for key := range subvectorToTSs {
		n := len(subvectorToTSs[key])
		for i := 0; i < n; i++ {
			candidates := []int{}
			for j := i + 1; j < n; j++ {
				candidates = append(candidates, subvectorToTSs[key][j])
			}
			idClusterChans[subvectorToTSs[key][i]] <- candidates
		}
	}
}

// count number of the same subvectors
func counter(id int, idClusterC <-chan []int, pairsC chan<- []IDPair) {
	// counting
	count := make(map[int]int)
	for i := 0; i < sketchSize/2; i++ {
		for _, candiID := range <-idClusterC {
			count[candiID]++
		}
	}

	// check fraction
	pairs := []IDPair{}
	for candiID, num := range count {
		if float64(num)/float64(sketchSize)*2 >= fractionThr {
			pairs = append(pairs, IDPair{id, candiID})
		}
	}

	pairsC <- pairs
}

// compute Pearson correlation (needs mean, stdev and sequence)
func computePearson(tsProfile1, tsProfile2 TimeSeriesProfile) float64 {
	n := len(tsProfile1.data)
	mean1, mean2 := tsProfile1.mean, tsProfile2.mean
	stdev1, stdev2 := tsProfile1.stdev, tsProfile2.stdev

	sum := 0.0
	for i := 0; i < n; i++ {
		delta1 := tsProfile1.data[i] - mean1
		delta2 := tsProfile2.data[i] - mean2
		sum += delta1 * delta2
	}

	return sum / float64(n) / stdev1 / stdev2
}
