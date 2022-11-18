package main

import (
	"log"
	"runtime"
)

// Linear Search for all windows of all time series
func linearSearch(allTimeSeries [][]float64) {
	timeSeriesSize := len(allTimeSeries[0])

	windowNum := (timeSeriesSize-windowSize)/basicWindowSize + 1
	for i := 0; i < windowNum; i++ {
		log.Println("Window: ", i)
		singleLinearSearch(allTimeSeries, basicWindowSize*i)
	}
}

// Linear Search for a single window of all time series
func singleLinearSearch(allTimeSeries [][]float64, startPos int) []IDPair {
	pairs := []IDPair{}
	timeSeriesNum := len(allTimeSeries)

	tsProfiles := make([]TimeSeriesProfile, timeSeriesNum)
	for i := 0; i < timeSeriesNum; i++ {
		mean, stdev := computeStats(allTimeSeries[i], startPos)
		tsProfiles[i] = TimeSeriesProfile{i, mean, stdev, []float64{}, allTimeSeries[i][startPos : startPos+windowSize]}
	}

	for i := 0; i < timeSeriesNum; i++ {
		for j := i + 1; j < timeSeriesNum; j++ {
			pearson := computePearson(tsProfiles[i], tsProfiles[j])
			if pearson >= pearsonThr {
				// log.Println(i, j, pearson)
				pairs = append(pairs, IDPair{i, j})
			}
		}
	}
	log.Println("Number of pairs found:", len(pairs))

	return pairs
}

// Linear Search for a window of a batch of time series
func batchLinearSearch(tsProfiles []TimeSeriesProfile, start, end int, startPos int, pairsChan chan []IDPair) {
	pairs := []IDPair{}
	totalNum := len(tsProfiles)

	for i := start; i < end; i++ {
		for j := i + 1; j < totalNum; j++ {
			pearson := computePearson(tsProfiles[i], tsProfiles[j])
			if pearson >= pearsonThr {
				pairs = append(pairs, IDPair{i, j})
			}
		}
	}

	pairsChan <- pairs
}

// Compute time series profiles for a single window of a batch of time series
func computeTSProfiles(allTimeSeries [][]float64, start, end, startPos int, tsProfilesC chan []TimeSeriesProfile) {
	tsProfiles := make([]TimeSeriesProfile, end-start)
	for i := start; i < end; i++ {
		mean, stdev := computeStats(allTimeSeries[i], startPos)
		tsProfiles[i-start] = TimeSeriesProfile{i, mean, stdev, []float64{}, allTimeSeries[i][startPos : startPos+windowSize]}
	}

	tsProfilesC <- tsProfiles
}

// Parallel Linear Search for a single window of all time series
func parallelSingleLinearSearch(allTimeSeries [][]float64, startPos int) []IDPair {
	timeSeriesNum := len(allTimeSeries)
	batchNum := runtime.NumCPU()
	batchSize := timeSeriesNum / batchNum

	// Comptue profiles (without sketch data)
	tsProfilesC := make(chan []TimeSeriesProfile, batchNum)
	for i := 0; i < batchNum; i++ {
		start, end := batchSize*i, batchSize*(i+1)
		if i == batchNum-1 {
			end = len(allTimeSeries)
		}
		go computeTSProfiles(allTimeSeries, start, end, startPos, tsProfilesC)
	}
	tsProfiles := []TimeSeriesProfile{}
	for i := 0; i < batchNum; i++ {
		tsProfiles = append(tsProfiles, <-tsProfilesC...)
	}

	// Brute Force Search
	pairsChan := make(chan []IDPair, batchNum)
	for i := 0; i < batchNum; i++ {
		start, end := batchSize*i, batchSize*(i+1)
		if i == batchNum-1 {
			end = len(tsProfiles)
		}
		go batchLinearSearch(tsProfiles, start, end, startPos, pairsChan)
	}

	pairs := []IDPair{}
	for i := 0; i < batchNum; i++ {
		pairs = append(pairs, <-pairsChan...)
	}

	log.Println("Number of pairs found:", len(pairs))

	return pairs
}

// func singleTSLinearSearch(tsProfile1 TimeSeriesProfile, ts2 int, c chan<- IDPair) {

// }
