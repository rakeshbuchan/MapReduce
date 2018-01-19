package mr.assignment2IMC;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ClimateAnalysisInMapperCombiner {

	// Mapper Class
	public static class ClimateAnalysisInMapperCombiner_Mapper
			extends Mapper<Object, Text, Text, TemperatureAccumulator> {

		// Data structure to store all computations performed at the map level
		Map<String, TemperatureAccumulator> mapComputations;

		// setup method to initialize map level data structure
		protected void setup(Context context) throws IOException, InterruptedException {
			mapComputations = new HashMap<String, TemperatureAccumulator>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] itr = value.toString().split("\n");
			// splitting each line based on commas
			for (String currentLine : itr) {
				String[] StationReading = currentLine.split(",");

				// Adding temperature and count values into the hashmap
				if (mapComputations.containsKey(StationReading[0])) {
					if (StationReading[2].equals("TMAX")) {
						mapComputations.get(StationReading[0]).updateTmaxTemp(Double.parseDouble(StationReading[3]));
					} else if (StationReading[2].equals("TMIN")) {
						mapComputations.get(StationReading[0]).updateTminTemp(Double.parseDouble(StationReading[3]));
					}
				} else {
					if (StationReading[2].equals("TMAX")) {
						mapComputations.put(StationReading[0],
								new TemperatureAccumulator(1, Double.parseDouble(StationReading[3]), 0, 0.0));
					} else if (StationReading[2].equals("TMIN")) {
						mapComputations.put(StationReading[0],
								new TemperatureAccumulator(0, 0.0, 1, Double.parseDouble(StationReading[3])));
					}
				}

			}
		}

		// cleanup method to emit mapper computations towards the end of mapper
		// execution
		protected void cleanup(Context context) throws IOException, InterruptedException {
			/*
			 * emitting key-value pairs for TMAX & TMIN records 
			 * key - StationID 
			 * value - TemperatureAccumulator class used as data structure which store station
			 * details
			 */
			for (Map.Entry<String, TemperatureAccumulator> entry : mapComputations.entrySet()) {
				context.write(new Text(entry.getKey()), entry.getValue());

			}
		}
	}

	// Reducer Class
	public static class ClimateAnalysisInMapperCombiner_Reducer
			extends Reducer<Text, TemperatureAccumulator, String, NullWritable> {

		private NullWritable result = NullWritable.get();
		public void reduce(Text key, Iterable<TemperatureAccumulator> values, Context context)
				throws IOException, InterruptedException {

			int TminCount = 0;
			int TmaxCount = 0;
			Double TminSum = 0.0;
			Double TmaxSum = 0.0;
			Double TminAvg = 0.0;
			Double TmaxAvg = 0.0;

			for (TemperatureAccumulator val : values) {
				if (val.getTmaxTempCount() >= 1) {
					// Calculating sum and counts of Tmax temperatures
					TmaxSum = TmaxSum + val.getTmaxTempSum();
					TmaxCount = TmaxCount + val.getTmaxTempCount();
				}
				if (val.getTminTempCount() >= 1) {
					// Calculating sum and counts of Tmin temperatures
					TminSum = TminSum + val.getTminTempSum();
					TminCount = TminCount + val.getTminTempCount();
				}
			}

			// Calculating average of Tmax & Tmin temperatures
			if (TmaxCount > 0)
				TmaxAvg = TmaxSum / TmaxCount;
			if (TminCount > 0)
				TminAvg = TminSum / TminCount;

			/*
			 * emitting final results from reducers - 
			 * Key - StationId0, MeanMinTemp0, MeanMaxTemp0
			 * Value - null
			 */
			String record = "" + key + ", " + TminAvg + ", " + TmaxAvg;
			context.write(record, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ClimateAnalysisInMapperCombiner <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "InMapperCombiner Average Min-Max");
		job.setJarByClass(ClimateAnalysisInMapperCombiner.class);
		job.setMapperClass(ClimateAnalysisInMapperCombiner_Mapper.class);
		job.setReducerClass(ClimateAnalysisInMapperCombiner_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TemperatureAccumulator.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
