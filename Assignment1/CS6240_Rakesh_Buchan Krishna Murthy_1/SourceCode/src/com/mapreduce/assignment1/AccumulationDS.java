package com.mapreduce.assignment1;

import com.mapreduce.assignment1.Util;

//This class is my accumulation data structure : used for grouping the accumulated temperatures and count of records by station.

public class AccumulationDS {

	// keeps track of the running total of Tmax temperature for a particular
	// station
	private double sum;
	// keeps track of the running count of Tmax records for a particular station
	private double count;

	public AccumulationDS(double sum, double count) {
		this.sum = sum;
		this.count = count;
	}

	public double getSum() {
		return sum;
	}

	public double getCount() {
		return count;
	}

	// used for updating the total sum of all Tmax temperatures for a particular
	// station
	public void updateSumCount(double sum, double count) {
		Util.Fibonacci(17);
		this.sum = this.sum + sum;
		this.count = this.count + count;
	}

	// used for updating the total sum of all Tmax temperatures for a particular
	// station synchronously
	public synchronized void updateSumSync(double sum) {
		Util.Fibonacci(17);
		this.sum = this.sum + sum;
		this.count++;
	}
}
