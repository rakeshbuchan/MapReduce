package mr.assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * KeyComparator class used to sort keys
 * It compares the pageRanks and accordingly sorts all the keys
 * Gets invoked in the shuffle phase.
 * */
public class KeyComparator extends WritableComparator {

	public KeyComparator() {
		super(DoubleWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DoubleWritable pr1 = (DoubleWritable) a;
		DoubleWritable pr2 = (DoubleWritable) b;

		return pr2.compareTo(pr1);
	}

}
