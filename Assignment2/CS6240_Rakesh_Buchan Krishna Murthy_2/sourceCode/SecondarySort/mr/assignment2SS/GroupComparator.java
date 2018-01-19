package mr.assignment2SS;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//GroupingComparator class, which groups keys based on StationIDs
//gets invoked at the shuffle phase after Key comparator and before Reducer
public class GroupComparator extends WritableComparator {

	public GroupComparator() {
		super(StationIdYearKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StationIdYearKey key1 = (StationIdYearKey) a;
		StationIdYearKey key2 = (StationIdYearKey) b;

		return key1.compareTo(key2);
	}

}
