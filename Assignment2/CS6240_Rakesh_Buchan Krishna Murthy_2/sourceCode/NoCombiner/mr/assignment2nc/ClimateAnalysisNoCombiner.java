package mr.assignment2nc;

import java.io.IOException;

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

public class ClimateAnalysisNoCombiner {

	// Mapper Class
	public static class ClimateAnalysisNoCombiner_Mapper extends Mapper<Object, Text, Text, TemperatureAccumulator> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] itr = value.toString().split("\n");
			// splitting each line based on commas
			for (String currentLine : itr) {
				String[] StationReading = currentLine.split(",");

				/*
				 * emitting key-value pairs for TMAX & TMIN records 
				 * key - StationID 
				 * value - TemperatureAccumulator class used as data structure which store station
				 * details
				 */
				if (StationReading[2].equals("TMAX")) {
					context.write(new Text(StationReading[0]),
							new TemperatureAccumulator(1, Double.parseDouble(StationReading[3])));
				} else if (StationReading[2].equals("TMIN")) {
					context.write(new Text(StationReading[0]),
							new TemperatureAccumulator(0, Double.parseDouble(StationReading[3])));
				}
			}
		}
	}

	// Reducer Class
	public static class ClimateAnalysisNoCombiner_Reducer extends Reducer<Text, TemperatureAccumulator, String, NullWritable> {

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
				if (val.getType() == 1) {
					// Calculating sum and counts of Tmax temperatures
					TmaxSum = TmaxSum + val.getTemperature();
					TmaxCount++;
				} else if (val.getType() == 0) {
					// Calculating sum and counts of Tmin temperatures
					TminSum = TminSum + val.getTemperature();
					TminCount++;
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
			System.err.println("Usage: ClimateAnalysisNoCombiner <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "NoCombiner Average Min-Max");
		job.setJarByClass(ClimateAnalysisNoCombiner.class);
		job.setMapperClass(ClimateAnalysisNoCombiner_Mapper.class);
		job.setReducerClass(ClimateAnalysisNoCombiner_Reducer.class);
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
