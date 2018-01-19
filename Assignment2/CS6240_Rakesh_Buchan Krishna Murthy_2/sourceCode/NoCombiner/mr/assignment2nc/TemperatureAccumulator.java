package mr.assignment2nc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Custom Data structure class to store temperature type & temperature value
public class TemperatureAccumulator implements Writable {

	private Integer type; // stores the value 1 for TMAX & 0 for TMIN
	private Double temperature; // stores the temperature value

	// constructors
	public TemperatureAccumulator(Integer type, Double temperature) {
		this.type = type;
		this.temperature = temperature;
	}

	public TemperatureAccumulator() {
		this.type = 0;
		this.temperature = 0.0;
	}

	// getters
	public Integer getType() {
		return type;
	}

	public Double getTemperature() {
		return temperature;
	}

	// setters
	public void setType(Integer type) {
		this.type = type;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	//Overridden methods
	public void write(DataOutput out) throws IOException {
		out.writeInt(getType());
		out.writeDouble(getTemperature());
	}

	public void readFields(DataInput in) throws IOException {
		this.type = in.readInt();
		this.temperature = in.readDouble();
	}
}