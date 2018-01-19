package mr.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopK {

	// Mapper Class
	public static class TopK_Mapper extends Mapper<Object, Text, DoubleWritable, PageData> {

		Map<String, Double> pageRankMap;// Hash map for storing each page and it's rank
		Integer top_k_count;

		// set up function to initialize the hash map, which will be used as in mapper
		// combining all the page data
		protected void setup(Context context) throws IOException, InterruptedException {
			pageRankMap = new HashMap<String, Double>();
			top_k_count = context.getConfiguration().getInt("top_k_count", 100);

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split("\n");
			// splitting each line based on new line
			for (String currentLine : itr) {
				// extracting page name and page rank from each line and put in the hash map
				String[] pageDetails = currentLine.split("#");
				String pageName = pageDetails[0].substring(0, pageDetails[0].indexOf('[') - 1).trim();
				Double pageRank = Double.parseDouble(pageDetails[1]);
				pageRankMap.put(pageName, pageRank);
			}
		}

		// clean up - used for emitting only the local topK
		protected void cleanup(Context context) throws IOException, InterruptedException {
			List<Entry<String, Double>> localPageList = new ArrayList<>(pageRankMap.entrySet());
			Collections.sort(localPageList, new Comparator<Entry<String, Double>>() {
				// sort all the entries in the hash map in the decreasing order of their ranks
				public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
					return (e2.getValue().compareTo(e1.getValue()));
				}

			});

			List<Entry<String, Double>> localTopK = localPageList.subList(0, top_k_count);

			for (Entry<String, Double> i : localTopK) {
				// emitting only the top K page data
				context.write(new DoubleWritable(i.getValue()), new PageData(i.getValue(), i.getKey()));
			}
		}
	}

	// Reducer Class
	public static class TopK_Reducer extends Reducer<DoubleWritable, PageData, Text, DoubleWritable> {

		public void reduce(DoubleWritable key, Iterable<PageData> values, Context context)
				throws IOException, InterruptedException {

			Integer top_k_count = context.getConfiguration().getInt("top_k_count", 100);
			List<PageData> result = new ArrayList<PageData>();
			int count = 1;
			for (PageData val : values) {
				// copy only top K input records
				result.add(val);
				count++;
				if (count > top_k_count) {
					break;
				}
			}

			for (PageData val : result) {
				// emitting only the top K records
				context.write(new Text(val.getPageName()), new DoubleWritable(val.getPageRank()));
			}

		}

	}
}
