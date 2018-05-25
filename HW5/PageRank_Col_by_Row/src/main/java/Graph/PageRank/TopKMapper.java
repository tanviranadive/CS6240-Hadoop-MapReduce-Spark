package Graph.PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.sun.tools.javac.util.List;

public class TopKMapper extends Mapper<LongWritable, MatrixCell, DoubleWritable, LongWritable> {

	// Maintain a treemap to store only the top 100 pages for each map task.
	// Stores pageRank as key and corresponding pages as values
	private TreeMap<Double, ArrayList<Long>> recordMap = new TreeMap<Double, ArrayList<Long>>();

	public void map(LongWritable row, MatrixCell rCell, Context context) throws IOException, InterruptedException {

		double pagerank = rCell.value;
		long pageId = row.get();

		// if there are multiple pages with same page rank, add the page to list
		// and put updated node back into treemap else out the node into the map
		if (recordMap.containsKey(pagerank)) {
			ArrayList<Long> pages = recordMap.get(pagerank);
			pages.add(pageId);
		} else {
			ArrayList<Long> pages = new ArrayList<Long>();
			pages.add(pageId);
			recordMap.put(pagerank, pages);
		}

		// if recordsize becomes more than 100, remove the smallest element from treemap
		if (recordMap.size() > 100) {
			recordMap.remove(recordMap.firstKey());
		}
	}

	// emit the top 100 records from the treemap
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Double d : recordMap.keySet()) {
			ArrayList<Long> pages = recordMap.get(d);
			for(long pageId: pages)
				context.write(new DoubleWritable(d), new LongWritable(pageId));
		}
	}

}
