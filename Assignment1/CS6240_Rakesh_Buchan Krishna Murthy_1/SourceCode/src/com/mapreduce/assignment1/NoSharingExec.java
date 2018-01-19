package com.mapreduce.assignment1;

import static com.mapreduce.assignment1.FileLoader.inputData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NoSharingExec {

	// hash map with key as StationID, value is running total of Tmax
	// tempearatures and Tmax record counts
	static HashMap<String, AccumulationDS> stationData;

	public void initiateProcessing(int noOfThreads) {

		// to store the Time taken for execution of the program
		ArrayList<Double> executionStats = new ArrayList<Double>();
		double totalExecTime = 0.0;

		for (int j = 0; j < 10; j++) {

			stationData = new HashMap<String, AccumulationDS>();

			double startTime = System.currentTimeMillis();

			NoSharingWorker processingChunk[] = new NoSharingWorker[noOfThreads];

			int startOffset = 0;
			int endOffset = inputData.size() / noOfThreads;

			for (int i = 0; i < noOfThreads; i++) {
				// creating workerthread objects based on the number of
				// processors
				processingChunk[i] = new NoSharingWorker(startOffset, endOffset);
				processingChunk[i].start();

				// recalculating startoffset and endoffset for each thread
				startOffset = endOffset;
				if (i == noOfThreads - 2) {
					// the endIndex for the last thread will be end of file
					endOffset = inputData.size();
				} else {
					endOffset = endOffset + inputData.size() / noOfThreads;
				}

			}

			// waiting for all threads to finish execution
			for (int i = 0; i < noOfThreads; i++) {
				try {
					processingChunk[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// Combining the output results of each thread
			for (int i = 0; i < noOfThreads; i++) {
				for (String tempStationDataKey : processingChunk[i].getStationData().keySet()) {
					if (stationData.containsKey(tempStationDataKey)) {
						double tempSum = processingChunk[i].getStationData().get(tempStationDataKey).getSum();
						double tempCount = processingChunk[i].getStationData().get(tempStationDataKey).getCount();
						stationData.get(tempStationDataKey).updateSumCount(tempSum, tempCount);
					} else {
						stationData.put(tempStationDataKey,
								processingChunk[i].getStationData().get(tempStationDataKey));
					}
				}

			}

			// Result hash map with key as StationID, value is average Tmax
			// evaluated at the end of all records processing
			HashMap<String, Double> resultList = new HashMap<String, Double>();
			Double tempSum;
			Double tempCount;
			Double average;

			// evaluating the final Tmax average for each station ID
			for (Map.Entry<String, AccumulationDS> entry : stationData.entrySet()) {
				String stationID = entry.getKey();
				AccumulationDS accumulationObject = entry.getValue();
				tempSum = accumulationObject.getSum();
				tempCount = accumulationObject.getCount();
				average = tempSum / tempCount;
				resultList.put(stationID, average);
			}

			/*
			 * for (Map.Entry<String, Double> entry : resultList.entrySet()) {
			 * System.out.println(entry.getKey() + " =  " + entry.getValue()); }
			 * 
			 * System.out.println("resultList size - "+resultList.size());
			 */

			resultList.clear();
			stationData.clear();

			double endTime = System.currentTimeMillis();
			double runningTime = endTime - startTime;
			totalExecTime = totalExecTime + runningTime;
			executionStats.add(runningTime);

		}

		Collections.sort(executionStats);

		System.out.println("No Sharing Minimum Execution Time : " + executionStats.get(0));
		System.out.println("No Sharing Maximum Execution Time : " + executionStats.get(9));
		System.out.println("No Sharing Average Execution Time : " + totalExecTime / 10);

	}
}
