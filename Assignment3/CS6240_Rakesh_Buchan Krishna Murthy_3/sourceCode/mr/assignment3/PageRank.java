package mr.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank {

	// Mapper Class
	public static class PageRank_Mapper extends Mapper<Object, Text, Text, MultiValueWritable> {

		private Integer currentIteration;
		private int pageCount;

		// setup function to set all the counters
		protected void setup(Context context) throws IOException, InterruptedException {
			currentIteration = context.getConfiguration().getInt("iteration", -1);
			pageCount = context.getConfiguration().getInt("totalPages", -1);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split("\n");
			// splitting each line based on new line
			for (String currentLine : itr) {
				// parsing each line to extract pageName, pageRank and outlinks
				String[] pageDetails = currentLine.split("#");
				String pageName = pageDetails[0].substring(0, pageDetails[0].indexOf('[') - 1).trim();
				Double pageRank = Double.parseDouble(pageDetails[1]);
				String outLinksList = pageDetails[0].substring(pageDetails[0].indexOf('[') + 1,
						pageDetails[0].length() - 2);
				String[] outLinks = outLinksList.split(", ");

				Double currentPageRank = 0.0;

				// for the first iteration, pageRank will be equally split among all pages
				if (currentIteration == 1) {
					currentPageRank = 1.0 / Double.valueOf(pageCount);
				} else {
					currentPageRank = pageRank;
				}

				// framing a string with all it's outlinks
				String outLinkValues = null;
				if (outLinks[0].length() != 0) {
					String tempOutLinkValues = "";
					for (String s : outLinks) {
						tempOutLinkValues = tempOutLinkValues + ", " + s;
					}
					outLinkValues = "[" + tempOutLinkValues.substring(2) + "]";

				} else {
					outLinkValues = "[]";
				}

				// creating a page object
				PageDetails page = new PageDetails(new Text(pageName), new DoubleWritable(currentPageRank),
						new Text(outLinkValues));

				// emitting the page
				context.write(new Text(pageName), new MultiValueWritable(page));

				if (outLinks[0].length() != 0) {
					// splitting the current's page rank among all it's outlinks
					Double splitRank = currentPageRank / Double.valueOf(outLinks.length);
					for (String outLink : outLinks) {
						// emitting each outlink page along with it's page rank share
						context.write(new Text(outLink), new MultiValueWritable(new DoubleWritable(splitRank)));
					}
				} else {
					// emitting a dangling node with key as dummy, which gets used for delta
					// calculation
					context.write(new Text("dummy"), new MultiValueWritable(new DoubleWritable(currentPageRank)));

				}

			}
		}
	}

	// Reducer Class
	public static class PageRank_Reducer extends Reducer<Text, MultiValueWritable, Text, Text> {

		double alpha;
		int pageCount;
		Double previous_delta;
		Double current_delta;

		// set up method with all counters set
		public void setup(Context context) {
			alpha = 0.15;
			pageCount = context.getConfiguration().getInt("totalPages", -1);
			previous_delta = Double.parseDouble(context.getConfiguration().get("previous_delta"));
			current_delta = Double.parseDouble(context.getConfiguration().get("current_delta"));
		}

		public void reduce(Text key, Iterable<MultiValueWritable> values, Context context)
				throws IOException, InterruptedException {
			Double rank = 0.0;
			PageDetails page = null;
			Double newPageRank = 0.0;

			// Calculates delta value for the current iteration from the all dangling nodes
			// contributions
			if (key.toString().equals("dummy")) {
				for (MultiValueWritable multiValueWritable : values) {
					Writable writable = multiValueWritable.get();
					if (writable instanceof DoubleWritable) {
						current_delta = current_delta + Double.parseDouble(writable.toString());
					}
				}
				// updates the final delta value of the current iteration
				context.getCounter(Driver.CountersEnum.CURRENT_DELTA).setValue(Double.doubleToLongBits(current_delta));
			} else {
				// for the actual pages as key, update their rank by summing up the scores
				// contributed by all it's inlinks
				for (MultiValueWritable multiValueWritable : values) {
					Writable writable = multiValueWritable.get();
					if (writable instanceof PageDetails) {

						//page = new PageDetails(((PageDetails) writable).getPageName(),
						//		((PageDetails) writable).getPageRank(), ((PageDetails) writable).getOutLinks());
						page = (PageDetails)writable;
					} else {
						if (writable instanceof DoubleWritable) {

							rank = rank + Double.parseDouble(writable.toString());

						}
					}
				}
				// calculate the new page rank considering the contributions of it's inlinks and
				// dangling pages and updates it's rank
				newPageRank = alpha / Double.valueOf(pageCount)
						+ (1 - alpha) * (previous_delta / Double.valueOf(pageCount) + rank);

				if(page != null) {
					page.setPageRank(new DoubleWritable(newPageRank));
					// emitting the page withupdated rank
					context.write(key, new Text(page.getOutLinks() + " #" + page.getPageRank()));
				}				
			}

		}
	}

}
