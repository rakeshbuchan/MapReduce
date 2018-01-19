package com.mapreduce.assignment1;

import static com.mapreduce.assignment1.FineLockExec.stationData;
import static com.mapreduce.assignment1.FileLoader.inputData;
import com.mapreduce.assignment1.Util;

//This class gets instantiated based on the number of parallel threads and each thread will have a startindex and endindex of the 
//input data list and process all the records between these indices

public class FineLockWorker extends Thread {
	private int startIndex;
	private int endIndex;

	public FineLockWorker(int start, int end) {
		this.startIndex = start;
		this.endIndex = end;
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
			if (stationData.containsKey(stationId)) {

				// calls the synchronized method to update sum
				stationData.get(stationId).updateSumSync(Double.parseDouble(reading));
			} else {

				// If two threads reach the else block at the same time for a
				// particular station ID, only one thread would be able to put
				// the key value pair in the HashMap since it doesn't allow
				// duplicate keys and it still holds the lock on the value and
				// the other thread would be able to update the value only after
				// this thread releases the lock, thus avoids overwriting
				AccumulationDS tempAccDS = new AccumulationDS(Double.parseDouble(reading), 1);

				// Acquires lock on tempAccDS object which is the value to be
				// inserted in hashmap
				synchronized (tempAccDS) {
					if (stationData.containsKey(stationId)) {
						stationData.get(stationId).updateSumSync(Double.parseDouble(reading));
					} else {
						Util.Fibonacci(17);
						stationData.put(stationId, tempAccDS);
					}
				}

			}

		}
	}
}
