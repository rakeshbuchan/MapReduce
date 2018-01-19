package mr.assignment3;



import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//Partitioner Class
//this ensures in routing all the records to a same single reducer
public class TopKPartitioner extends Partitioner<DoubleWritable, PageData> {

	@Override
	public int getPartition(DoubleWritable a, PageData b,	int numPartitions) {
		return 0;
	}

}

