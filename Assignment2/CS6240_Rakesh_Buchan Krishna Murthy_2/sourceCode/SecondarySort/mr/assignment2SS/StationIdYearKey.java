package mr.assignment2SS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//Composite Key class to store year and StationID
public class StationIdYearKey implements Writable, WritableComparable<StationIdYearKey> {

	private Integer year;
	private String stationID;

	// Constructors
	public StationIdYearKey(Integer year, String stationID) {
		this.year = year;
		this.stationID = stationID;
	}

	public StationIdYearKey() {
		this.year = 0;
		this.stationID = "";
	}

	// getters
	public Integer getYear() {
		return year;
	}

	public String getStationID() {
		return stationID;
	}

	// setters
	public void setYear(Integer year) {
		this.year = year;
	}

	public void setStationID(String stationID) {
		this.stationID = stationID;
	}

	// overridden methods
	public void write(DataOutput out) throws IOException {
		out.writeInt(getYear());
		out.writeBytes(getStationID());
	}

	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.stationID = in.readLine();
	}

	@Override
	public int compareTo(StationIdYearKey stationYearKey) {
		return this.stationID.compareTo(stationYearKey.getStationID());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationID == null) ? 0 : stationID.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StationIdYearKey other = (StationIdYearKey) obj;
		if (stationID == null) {
			if (other.stationID != null)
				return false;
		} else if (!stationID.equals(other.stationID))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

}
