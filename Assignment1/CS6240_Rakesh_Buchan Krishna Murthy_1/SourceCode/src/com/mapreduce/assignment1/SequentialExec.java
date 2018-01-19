package com.mapreduce.assignment1;

import java.util.*;
import static com.mapreduce.assignment1.FileLoader.inputData;
import com.mapreduce.assignment1.Util;

public class SequentialExec {
	public void evaluateAverage() {

		// to store the Time taken for execution of the program
		ArrayList<Double> executionStats = new ArrayList<Double>();
		double totalExecTime = 0.0;

		for (int j = 0; j < 10; j++) {

			// hash map with key as StationID, value is running total of Tmax
			// tempearatures and Tmax record counts
			HashMap<String, AccumulationDS> stationDetails = new HashMap<String, AccumulationDS>();

			// Result hash map with key as StationID, value is average Tmax
			// evaluated at the end of all records processing
			HashMap<String, Double> resultList = new HashMap<String, Double>();

			Double tempSum;
			Double tempCount;
			Double average;

			double startTime = System.currentTimeMillis();

			// splitting input csv file and extracting stationID and reading
			// values only for Tmax records
			for (String input : inputData) {
				if (input.contains("TMAX")) {
					String[] splits = input.split(",");
					String stationId = splits[0];
					String reading = splits[3];

					// adding the Tmax record details into the stationDetails
					// hashmap
					if (stationDetails.containsKey(stationId)) {
						stationDetails.get(stationId).updateSumCount(Double.parseDouble(reading), 1);
					} else {
						Util.Fibonacci(17);
						stationDetails.put(stationId, new AccumulationDS(Double.parseDouble(reading), 1));
					}
				}
			}

			// evaluating the final Tmax average for each station ID
			for (Map.Entry<String, AccumulationDS> entry : stationDetails.entrySet()) {
				String stationID = entry.getKey();
				AccumulationDS accumulationObject = entry.getValue();
				tempSum = accumulationObject.getSum();
				tempCount = accumulationObject.getCount();
				average = tempSum / tempCount;
				resultList.put(stationID, average);
			}

			double endTime = System.currentTimeMillis();
			double runningTime = endTime - startTime;
			totalExecTime = totalExecTime + runningTime;
			executionStats.add(runningTime);

			/*
			 * for (Map.Entry<String, Double> entry : resultList.entrySet()) {
			 * System.out.println(entry.getKey() + " =  " + entry.getValue()); }
			 * 
			 * System.out.println("resultList size - "+resultList.size());
			 */
		}

		Collections.sort(executionStats);

		System.out.println("Sequential Minimum Execution Time : " + executionStats.get(0));
		System.out.println("Sequential Maximum Execution Time : " + executionStats.get(9));
		System.out.println("Sequential Average Execution Time : " + totalExecTime / 10);

	}
}
