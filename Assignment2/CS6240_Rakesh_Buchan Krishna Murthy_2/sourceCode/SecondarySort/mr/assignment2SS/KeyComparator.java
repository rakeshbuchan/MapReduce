package mr.assignment2SS;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * KeyComparator class used to sort keys
 * It firstly compares the StationIDs
 * If they both are same, it then compares years
 * and accordingly sorts all the keys
 * Gets invoked in the shuffle phase, after Partitioner and before Grouping Comparator
 * */
public class KeyComparator extends WritableComparator {

	public KeyComparator() {
		super(StationIdYearKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StationIdYearKey key1 = (StationIdYearKey) a;
		StationIdYearKey key2 = (StationIdYearKey) b;

		int stationIDDiff = key1.getStationID().compareTo(key2.getStationID());

		if (stationIDDiff == 0) {
			return key1.getYear().compareTo(key2.getYear());
		} else
			return stationIDDiff;
	}

}
