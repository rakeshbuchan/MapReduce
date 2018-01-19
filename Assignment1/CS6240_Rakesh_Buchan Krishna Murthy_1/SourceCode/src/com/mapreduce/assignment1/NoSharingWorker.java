package com.mapreduce.assignment1;

import static com.mapreduce.assignment1.FileLoader.inputData;
import java.util.HashMap;
import com.mapreduce.assignment1.Util;

//This class gets instantiated based on the number of parallel threads and each thread will have a startindex and endindex of the 
//input data list and process all the records between these indices

public class NoSharingWorker extends Thread {
	private int startIndex;
	private int endIndex;

	// Own copy of data structure for each thread
	private HashMap<String, AccumulationDS> stationDataSplit = new HashMap<String, AccumulationDS>();

	public NoSharingWorker(int start, int end) {
		this.startIndex = start;
		this.endIndex = end;
	}

	public HashMap<String, AccumulationDS> getStationData() {
		return stationDataSplit;
	}

	public void run() {
		for (int i = startIndex; i < endIndex; i++) {
			evaluateAverage(i);
		}

	}

	private void evaluateAverage(int index) {
		String currentLine = inputData.get(index);
		if (currentLine.contains("TMAX")) {
			// splitting input csv file and extracting stationID and reading
			// values only for Tmax records
			String[] splits = currentLine.split(",");
			String stationId = splits[0];
			String reading = splits[3];

			// adding the Tmax record details into the stationDetails
			// hashmap
			if (stationDataSplit.containsKey(stationId)) {
				stationDataSplit.get(stationId).updateSumCount(Double.parseDouble(reading), 1);
			} else {
				Util.Fibonacci(17);
				stationDataSplit.put(stationId, new AccumulationDS(Double.parseDouble(reading), 1));
			}
		}
	}
}
