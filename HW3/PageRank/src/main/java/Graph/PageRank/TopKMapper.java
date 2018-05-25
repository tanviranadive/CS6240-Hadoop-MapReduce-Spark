package Graph.PageRank;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopKMapper extends Mapper<Text, PageData, DoubleWritable, Text> {

	// Maintain a treemap to store only the top 100 pages for each map task.
	// Stores pageRank as key and corresponding pages as values
	private TreeMap<Double, String> recordMap = new TreeMap<Double, String>();

	public void map(Text pageText, PageData pageData, Context context) throws IOException, InterruptedException {

		double pagerank = pageData.getPageRank();

		// if there are multiple pages with same page rank, add the page to list
		// and put updated node back into treemap else out the node into the map
		if (recordMap.containsKey(pagerank)) {
			String pagename = recordMap.get(pagerank);
			pagename += " , " + pageText.toString();
			recordMap.put(pagerank, pagename);
		} else {
			recordMap.put(pagerank, pageText.toString());
		}

		// if recordsize becomes more than 100, remove the smallest element from treemap
		if (recordMap.size() > 100) {
			recordMap.remove(recordMap.firstKey());
		}
	}

	// emit the top 100 records from the treemap
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Double d : recordMap.keySet()) {
			String pageName = recordMap.get(d).toString();
			context.write(new DoubleWritable(d), new Text(pageName));
		}
	}

}
