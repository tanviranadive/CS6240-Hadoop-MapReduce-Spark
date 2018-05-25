package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Page Rank Mapper takes in (pageName, PageData) and updates the page rank in PageData
public class PageRankMapper extends Mapper<Text, PageData, Text, PageData> {

	static int iteration;
	static float alpha;
	static long totalPages;
	static double initialPageRank;
	static int count;

	// get the hadoop couters
	public void setup(Context context) {
		iteration = Integer.parseInt(context.getConfiguration().get("iteration"));
		alpha = Float.parseFloat(context.getConfiguration().get("alpha"));
		totalPages = Long.parseLong(context.getConfiguration().get("totalPages"));
		count= 0;
	}

	public void map(Text pageText, PageData pageData, Context context) throws IOException, InterruptedException {
		
		count++;
		if(iteration==0) {
			PageData pData = new PageData();
			pData.setPageRank(1.0/totalPages);
			pData.setOutlinks(pageData.getPageList());
			context.write(pageText, pData);
			return;
		}
		
		String pageName = pageText.toString();
		double pageRank = pageData.getPageRank();

		// Write pageName and outlinks to recover graph in reducer. Set 0 in PageRank as dummy value
		PageData pdata = new PageData();
		
		pdata.set(0, pageData.getPageList());

		context.write(pageText, pdata);
		
		String[] outlinksList = pageData.getPageList().split(",");

		// calculate page rank if not a sink node
		if (!pageData.getPageList().equals("")) {
			double pageRankContributedByCurrent = pageRank / outlinksList.length;

			for (int j = 0; j < outlinksList.length; j++) {
				if (!outlinksList[j].equals("")) {
					PageData outLinkPage = new PageData();
					outLinkPage.set(pageRankContributedByCurrent, "");

					// emit (pageName,(pageRank,"")) dummy value
					context.write(new Text(outlinksList[j].trim()), outLinkPage);
				}
			}
		}
	}

}
