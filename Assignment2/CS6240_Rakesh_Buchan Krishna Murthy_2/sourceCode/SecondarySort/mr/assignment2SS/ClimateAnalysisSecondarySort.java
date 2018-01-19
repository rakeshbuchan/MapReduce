package mr.assignment2SS;

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

public class ClimateAnalysisSecondarySort {

	// Mapper Class
	public static class ClimateAnalysisSecondarySort_Mapper
			extends Mapper<Object, Text, StationIdYearKey, TemperatureAccumulator> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] itr = value.toString().split("\n");
			// splitting each line based on commas
			for (String currentLine : itr) {
				String[] StationReading = currentLine.split(",");

				/*
				 * emitting key-value pairs for TMAX & TMIN records 
				 * key - Year, StationID 
				 * value - TemperatureAccumulator class used as data structure which store station
				 * details
				 */
				if (StationReading[2].equals("TMAX")) {
					Integer year = Integer.parseInt(StationReading[1].substring(0, 4));
					context.write(new StationIdYearKey(year, StationReading[0]),
							new TemperatureAccumulator(1, Double.parseDouble(StationReading[3]), 0, 0.0));
				} else if (StationReading[2].equals("TMIN")) {
					Integer year = Integer.parseInt(StationReading[1].substring(0, 4));
					context.write(new StationIdYearKey(year, StationReading[0]),
							new TemperatureAccumulator(0, 0.0, 1, Double.parseDouble(StationReading[3])));
				}
			}
		}
	}

	// Combiner Class
	// gets invoked at the mapper at the end of it's computations
	public static class ClimateAnalysisSecondarySort_Combiner
			extends Reducer<StationIdYearKey, TemperatureAccumulator, StationIdYearKey, TemperatureAccumulator> {

		public void reduce(StationIdYearKey key, Iterable<TemperatureAccumulator> values, Context context)
				throws IOException, InterruptedException {
			int TminCount = 0;
			int TmaxCount = 0;
			Double TminSum = 0.0;
			Double TmaxSum = 0.0;
			for (TemperatureAccumulator val : values) {
				TmaxSum = TmaxSum + val.getTmaxTempSum();
				TmaxCount = TmaxCount + val.getTmaxTempCount();
				TminSum = TminSum + val.getTminTempSum();
				TminCount = TminCount + val.getTminTempCount();
			}
			/*
			 * emitting key-value pairs for TMAX & TMIN records at combiner at the end of 
			 * each map operation
			 * key - StationID
			 * value - TemperatureAccumulator class used as data structure which store
			 * station details
			 */
			context.write(key, new TemperatureAccumulator(TmaxCount, TmaxSum, TminCount, TminSum));
		}
	}

	// Reducer Class
	public static class ClimateAnalysisSecondarySort_Reducer
			extends Reducer<StationIdYearKey, TemperatureAccumulator, String, NullWritable> {

		private NullWritable result = NullWritable.get();

		public void reduce(StationIdYearKey key, Iterable<TemperatureAccumulator> values, Context context)
				throws IOException, InterruptedException {

			int TminCount = 0;
			int TmaxCount = 0;
			Double TminSum = 0.0;
			Double TmaxSum = 0.0;
			Double TminAvg = 0.0;
			Double TmaxAvg = 0.0;

			int currentYear = key.getYear();
			int previousYear = currentYear;

			StringBuilder record = new StringBuilder();
			record.append(key.getStationID() + ", [");

			for (TemperatureAccumulator val : values) {
				previousYear = currentYear;
				currentYear = key.getYear();

				if (previousYear != currentYear) {
					//When the year changes, calculate averages and reset the counters
					record.append("(" + previousYear + ", ");
					if (TminCount > 0)
						TminAvg = TminSum / TminCount;
					else
						TminAvg = 0.0;
					record.append(TminAvg + ", ");
					if (TmaxCount > 0)
						TmaxAvg = TmaxSum / TmaxCount;
					else
						TmaxAvg = 0.0;
					record.append(TmaxAvg + "), ");

					TminCount = 0;
					TmaxCount = 0;
					TminSum = 0.0;
					TmaxSum = 0.0;
					TminAvg = 0.0;
					TmaxAvg = 0.0;

				}
				
				//updating the counters for current year
				TmaxSum = TmaxSum + val.getTmaxTempSum();
				TmaxCount = TmaxCount + val.getTmaxTempCount();
				TminSum = TminSum + val.getTminTempSum();
				TminCount = TminCount + val.getTminTempCount();
			}

			//calculate averages for the final year record and complete creating output record
			record.append("(" + currentYear + ", ");
			if (TminCount > 0)
				TminAvg = TminSum / TminCount;
			else
				TminAvg = 0.0;
			record.append(TminAvg + ", ");
			if (TmaxCount > 0)
				TmaxAvg = TmaxSum / TmaxCount;
			else
				TmaxAvg = 0.0;
			record.append(TmaxAvg + ")]");

			/*
			 * emitting final results from reducers - 
			 * Key - StationIda, [(1880, MeanMina0, MeanMaxa0), (1881, MeanMina1, MeanMaxa1) ...(1889 ...)]
			 * Value - null
			 */

			context.write(record.toString(), result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ClimateAnalysisSecondarySort <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SecondarySort Average Min-Max");
		job.setJarByClass(ClimateAnalysisSecondarySort.class);

		job.setMapperClass(ClimateAnalysisSecondarySort_Mapper.class);
		job.setCombinerClass(ClimateAnalysisSecondarySort_Combiner.class);

		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(HashPartitioner.class);

		job.setReducerClass(ClimateAnalysisSecondarySort_Reducer.class);

		job.setMapOutputKeyClass(StationIdYearKey.class);
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
