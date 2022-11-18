package main

import (
	"log"
	"runtime"
	"time"
)

// Some parameters
var (
	windowSize      = 50
	basicWindowSize = 20
	sketchSize      = 10
	gridSize        = 10
	pearsonThr      = 0.7
	fractionThr     = 0.5
)

// Compute sketches and search similarity for all the time series across all the query windows
func parcorrSearch(allTimeSeries [][]float64) {

	// Special processing for the first windows
	_, randomMatrix, slidingRandomMatrixSum, tsProfiles := firstParcorrSearch(allTimeSeries)

	// Search the following windows
	timeSeriesSize := len(allTimeSeries[0])
	windowNum := (timeSeriesSize-windowSize)/basicWindowSize + 1
	for i := 1; i < windowNum; i++ {
		log.Println("Window:", i)
		_, randomMatrix, slidingRandomMatrixSum, tsProfiles, _, _ = singleParcorrSearch(allTimeSeries, i*basicWindowSize, randomMatrix, slidingRandomMatrixSum, tsProfiles)
	}
}

// Special processing for the first ParCorr Search
func firstParcorrSearch(allTimeSeries [][]float64) (pairs []IDPair, randomMatrix [][]int8, slidingRandomMatrixSum []int, tsProfiles []TimeSeriesProfile) {
	timeSeriesNum := len(allTimeSeries)

	randomMatrix = computeRandomMatrix(sketchSize, windowSize+basicWindowSize)

	slidingRandomMatrixSum = make([]int, sketchSize)
	for i := 0; i < sketchSize; i++ {
		for j := 0; j < windowSize; j++ {
			slidingRandomMatrixSum[i] += int(randomMatrix[i][j])
		}
	}

	c := make(chan TimeSeriesProfile, timeSeriesNum)
	for i := 0; i < timeSeriesNum; i++ {
		go computeSketch(i, allTimeSeries[i], randomMatrix, c)
	}

	tsProfiles = make([]TimeSeriesProfile, timeSeriesNum)
	for i := 0; i < timeSeriesNum; i++ {
		tsProfile := <-c
		id := tsProfile.id
		tsProfiles[id] = tsProfile
	}

	pairs = ParallelMixing(tsProfiles)
	log.Println("The First Window is Completed")
	return
}

// normal ParCorr search after the first search
func singleParcorrSearch(allTimeSeries [][]float64, startPos int, randomMatrix [][]int8, slidingRandomMatrixSum []int, tsProfiles []TimeSeriesProfile) (pairs []IDPair, newRandomMatrix [][]int8, newSlidingRandomMatrixSum []int, newTSProfiles []TimeSeriesProfile, sketchDuration, searchDuration time.Duration) {
	timeSeriesNum := len(allTimeSeries)
	batchNum := runtime.NumCPU()
	batchSize := timeSeriesNum / batchNum

	startTime := time.Now()
	tsProfilesChan := make(chan []TimeSeriesProfile, batchNum)
	newSlidingRandomMatrixSum = updateSlidingRMSum(slidingRandomMatrixSum, randomMatrix)
	for i := 0; i < batchNum; i++ {
		start, end := batchSize*i, batchSize*(i+1)
		if i == batchNum-1 {
			end = timeSeriesNum
		}
		go updateBatchSketches(allTimeSeries, start, end, slidingRandomMatrixSum, newSlidingRandomMatrixSum, randomMatrix, tsProfiles, startPos, tsProfilesChan)
	}
	newRandomMatrix = updateRandomMatrix(randomMatrix, basicWindowSize)
	newTSProfiles = make([]TimeSeriesProfile, timeSeriesNum)
	for i := 0; i < batchNum; i++ {
		batchTSProfiles := <-tsProfilesChan
		startID := batchTSProfiles[0].id
		copy(newTSProfiles[startID:], batchTSProfiles)
	}
	sketchDuration = time.Since(startTime)
	log.Println("Sketches update completed, Costs", sketchDuration)

	startTime = time.Now()
	pairs = ParallelMixing(newTSProfiles)
	searchDuration = time.Since(startTime)
	log.Println("Search completed, Costs", searchDuration)

	return
}
