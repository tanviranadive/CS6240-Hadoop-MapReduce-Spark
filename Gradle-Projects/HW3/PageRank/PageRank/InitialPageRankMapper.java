package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/* Initial Page Rank Mapper to assign initial page rank value to each page and create format 
 * (pageName, (pageRank, outlinks))*/
public class InitialPageRankMapper extends Mapper<Text, PageData, Text, PageData> {

	static double initialPageRank;
	static double initialsinkSum;

	public void setup(Context context) {

		// assign intial pageRank value 1/total number of pages and initialize sink sum
		initialPageRank = 1 / Double.parseDouble(context.getConfiguration().get("totalPages"));
		initialsinkSum = 0.0d;
	}

	public void map(Text pageText, PageData pageData, Context context) throws IOException, InterruptedException {
		
		String  value = pageData.getPageList();
		// Data structure for storing new pageRank and outlinks
		pageData.set(initialPageRank, value);
		
		// if no outlinks, it is a sink node. Hence increase the sink sum by its page rank
		if (value.trim().equals("")) {
			initialsinkSum += pageData.getPageRank();
		}

		context.write(pageText, pageData);

	}

	public void cleanup(Context context) {

		// set the initial sink sum hadoop counter. Multiply by 100000000 to maintain precision
		// since hadoop counter is long and sink sum is actually double
		long ssum = (long) initialsinkSum * 100000000;
		context.getCounter("", "initialSinkSum").increment(ssum);
	}
}
