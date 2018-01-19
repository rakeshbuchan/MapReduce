package mr.assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/*This class contains the different type of objects which an mapper class can emit*/

public class MultiValueWritable extends GenericWritable {

	private static Class[] CLASSES = new Class[] {

			DoubleWritable.class, // used for storing page rank values
			PageDetails.class // used for storing page objects
	};

	public MultiValueWritable() {
	}

	public MultiValueWritable(Writable value) {
		set(value);
	}

	protected Class[] getTypes() {
		return CLASSES;
	}
}
