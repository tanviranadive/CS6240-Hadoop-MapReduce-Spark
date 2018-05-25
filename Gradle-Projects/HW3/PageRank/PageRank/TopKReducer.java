package PageRank;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	// counter for number of records written to output
	private int num;

	public void setup(Context context) {
		num = 0;
	}

	// SInce we have used a key comparator which sorts in descending order, the data
	// to the
	// reducer comes in descending order of the key, i.e page rank.
	// Now, just emit the first 100 records since they are already sorted in
	// descending order.
	public void reduce(DoubleWritable key, Iterable<Text> pages, Context context)
			throws IOException, InterruptedException {

		double pagerank = key.get();

		for (Text values : pages) {
			String[] pageList = values.toString().split(",");
			// if more than one page with same page rank emit it as seperate record
			for (String page : pageList) {
				// if records emitted reaches 100, stop the computation
				if (num > 100) {
					return;
				}

				context.write(new Text(page), key);
				num++;
			}

		}

	}
}
