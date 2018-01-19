package mr.assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

	public static final int MAX_ITERATIONS = 10;
	public static final int TOP_K_COUNT = 100;

	static int TOTAL_PAGES;

	static String CURRENT_DELTA = "0.0";
	static String PREVIOUS_DELTA = "0.0";

	// enum used as a custom counter to store total_pages and current and previous
	// delta values
	public static enum CountersEnum {
		TOTAL_PAGES, CURRENT_DELTA, PREVIOUS_DELTA
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: pageRank <in> [<in>...] <out>");
			System.exit(2);
		}

		// Preprocessing job
		Job preProcessingjob = Job.getInstance(conf, "Page Rank - Preprocessing");
		preProcessingjob.setJarByClass(Driver.class);

		preProcessingjob.setMapperClass(PreProcessor.PreprocessingJob_Mapper.class);
		preProcessingjob.setCombinerClass(PreProcessor.PreprocessingJob_Combiner.class);
		preProcessingjob.setReducerClass(PreProcessor.PreprocessingJob_Reducer.class);

		preProcessingjob.setMapOutputKeyClass(Text.class);
		preProcessingjob.setMapOutputValueClass(Text.class);

		preProcessingjob.setOutputKeyClass(Text.class);
		preProcessingjob.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(preProcessingjob, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(preProcessingjob, new Path(otherArgs[otherArgs.length - 1] + "1"));

		if (!preProcessingjob.waitForCompletion(true)) {
			throw new Exception("Preprocessing job failed");
		}

		// set total pages after pre processing
		TOTAL_PAGES = (int) preProcessingjob.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		conf.setInt("totalPages", TOTAL_PAGES);

		conf.set("previous_delta", "0.0");
		conf.set("current_delta", "0.0");

		// page rank evaluation for max_iteration times
		for (int i = 1; i <= MAX_ITERATIONS; i++) {
			conf.setInt("iteration", i);

			// Page Rank Job
			Job pageRankjob = Job.getInstance(conf, "Page Rank - Evaluation");
			pageRankjob.setJarByClass(Driver.class);

			pageRankjob.setMapperClass(PageRank.PageRank_Mapper.class);
			pageRankjob.setReducerClass(PageRank.PageRank_Reducer.class);

			pageRankjob.setMapOutputKeyClass(Text.class);
			pageRankjob.setMapOutputValueClass(MultiValueWritable.class);

			pageRankjob.setOutputKeyClass(Text.class);
			pageRankjob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(pageRankjob, new Path(otherArgs[otherArgs.length - 1] + i));
			FileOutputFormat.setOutputPath(pageRankjob, new Path(otherArgs[otherArgs.length - 1] + (i + 1)));

			if (!pageRankjob.waitForCompletion(true)) {
				throw new Exception("PageRank Evaluation job failed for iteration - " + i);
			}

			// setting current delta value to previous delta value at the end of each
			// iteration, so that it gets distributed in the successive iteration and
			// resetting current delta value
			Counters counter = pageRankjob.getCounters();
			Long cur_delta = counter.findCounter(CountersEnum.CURRENT_DELTA).getValue();
			Double cur_delta_double = Double.longBitsToDouble(cur_delta);
			conf.set("previous_delta", cur_delta_double.toString());
			conf.set("current_delta", "0.0");

		}

		// Job for finding top K
		Job topKJob = Job.getInstance(conf, "Page Rank - Top K");
		conf.setInt("top_k_count", TOP_K_COUNT);
		topKJob.setJarByClass(Driver.class);

		topKJob.setMapperClass(TopK.TopK_Mapper.class);
		topKJob.setReducerClass(TopK.TopK_Reducer.class);

		topKJob.setMapOutputKeyClass(DoubleWritable.class);
		topKJob.setMapOutputValueClass(PageData.class);

		topKJob.setSortComparatorClass(KeyComparator.class);
		topKJob.setPartitionerClass(TopKPartitioner.class);

		topKJob.setOutputKeyClass(Text.class);
		topKJob.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(topKJob,
				new Path(otherArgs[otherArgs.length - 1] + (conf.getInt("iteration", 11) - 1)));
		FileOutputFormat.setOutputPath(topKJob, new Path(otherArgs[otherArgs.length - 1] + "_topKResults"));
		System.exit(topKJob.waitForCompletion(true) ? 0 : 1);

	}
}
