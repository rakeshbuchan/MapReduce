package mr.assignment2SS;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Custom Data structure class to store temperature type & temperature value
public class TemperatureAccumulator implements Writable {
	
	private Integer tmaxTempCount; // stores count of Tmax temperatures
	private Double tmaxTempSum; // stores the sum of Tmax temperatures
	private Integer tminTempCount;	// stores count of Tmin temperatures
	private Double tminTempSum; // stores the sum of Tmin temperatures

	// constructors
	public TemperatureAccumulator(Integer tmaxTempCount, Double tmaxTempSum, Integer tminTempCount, Double tminTempSum) {
		this.tmaxTempCount = tmaxTempCount;
		this.tminTempCount = tminTempCount;
		this.tmaxTempSum = tmaxTempSum;
		this.tminTempSum = tminTempSum;
	}
	
	public TemperatureAccumulator() {
		this.tmaxTempCount = 0;
		this.tminTempCount = 0;
		this.tmaxTempSum = 0.0;
		this.tminTempSum = 0.0;
	}

	//getters
	public Integer getTmaxTempCount() {
		return tmaxTempCount;
	}
	
	public Double getTmaxTempSum() {
		return tmaxTempSum;
	}
	
	public Integer getTminTempCount() {
		return tminTempCount;
	}
	
	public Double getTminTempSum() {
		return tminTempSum;
	}

	//setters
	public void setTmaxTempCount(Integer tmaxTempCount) {
		this.tmaxTempCount = tmaxTempCount;
	}	

	public void setTmaxTempSum(Double tmaxTempSum) {
		this.tmaxTempSum = tmaxTempSum;
	}	

	public void setTminTempCount(Integer tminTempCount) {
		this.tminTempCount = tminTempCount;
	}	

	public void setTminTempSum(Double tminTempSum) {
		this.tminTempSum = tminTempSum;
	}
	
	//methods to update Tmin & Tmax temperatures and counts
	public void updateTmaxTemp(Double tmaxTemp) {
		this.tmaxTempSum = this.tmaxTempSum + tmaxTemp;
		this.tmaxTempCount++;
	}
	
	public void updateTminTemp(Double tminTemp) {
		this.tminTempSum = this.tminTempSum + tminTemp;
		this.tminTempCount++;
	}

	//Overridden methods
	public void write(DataOutput out) throws IOException {
		out.writeInt(getTmaxTempCount());
		out.writeDouble(getTmaxTempSum());
		out.writeInt(getTminTempCount());
		out.writeDouble(getTminTempSum());
	}

	public void readFields(DataInput in) throws IOException {
		this.tmaxTempCount = in.readInt();
		this.tmaxTempSum = in.readDouble();
		this.tminTempCount = in.readInt();
		this.tminTempSum = in.readDouble();
	}
}


