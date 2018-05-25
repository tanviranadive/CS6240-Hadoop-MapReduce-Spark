package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// Reducer for Page Rank
public class PageRankReducer extends Reducer<Text, PageData, Text, PageData> {

	static float alpha;
	static long totalPages;
	static long previousSinkSum;
	private double sinkSum;
	static int i;
	static double initialSinkSum;

	// get the context variables
	public void setup(Context context) {
		alpha = Float.parseFloat(context.getConfiguration().get("alpha"));
		totalPages = Long.parseLong(context.getConfiguration().get("totalPages"));
		previousSinkSum = context.getConfiguration().getLong("previousSinkSum", -10);
		sinkSum = 0.0d;
		i = context.getConfiguration().getInt("iteration", -10);
		initialSinkSum = 0.0d;
	}

	// receives (pageName, (0,outlinks)) and (pageName,(pageRank, ""))
	public void reduce(Text key, Iterable<PageData> pdata, Context context) throws IOException, InterruptedException {
		if(i == 0) {
			for(PageData pd: pdata) {
				if(pd.getPageList().equals("")) {
					sinkSum += pd.getPageRank();
				}
				context.write(key, pd);
			}
		}
		
		else {
		
		double newPageRank = 0;
		PageData pageData = new PageData();

		for (PageData pd : pdata) {
			// increment the new page rank
			if (pd.getPageRank() != 0) {
				newPageRank += pd.getPageRank();
			}

			// If its a node, recover the graph
			if (pd.getPageRank() == 0 && !pd.getPageList().equals("")) {
				pageData.setOutlinks(pd.getPageList());
			}
		}

		// calculate new page rank for the page
		newPageRank = (double) (alpha / totalPages) + ((1 - alpha) * newPageRank);
		newPageRank += (1.0 - alpha) * previousSinkSum / (totalPages * 100000000.0);
		pageData.setPageRank(newPageRank);

		// if its a sink node, increment sink sum for iteration
		if (pageData.getPageList().equals("")) {
			sinkSum += pageData.getPageRank();
		}

		// emit the page node and its data with updated new page rank
		context.write(key, pageData);
		}
	}

	// set the hadoop counter for sink sum of iteration i. Multiply by 1000000000
	// for precision since the
	// hadoop counter is long and sink sum is double
	public void cleanup(Context context) {
		long ssum = (long) (sinkSum * 100000000);
		context.getCounter("", "newSinkSum" + i).increment(ssum);
	}
}
