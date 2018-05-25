package Graph.PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<DoubleWritable, LongWritable, Text, DoubleWritable> {

	// counter for number of records written to output
	private int num;
	private Map<Long, String> pageMapper;

	public void setup(Context context) throws IOException {
		num = 0;
		pageMapper = new HashMap<Long, String>();
		
		loadPageMapping(context);
	}
	
	public void loadPageMapping(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();
		if(cacheFiles != null && cacheFiles.length > 0) {
			try {					
				FileSystem fs = FileSystem.get(context.getConfiguration());
				for(URI cacheFile: cacheFiles) {
					readFile(cacheFile, fs);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void readFile(URI cacheFile, FileSystem fs) {
		try {
			Path path = new Path(cacheFile.toString());
	        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
	        
	        String line;
			
	        while((line = reader.readLine()) != null) {
				String[] parts = line.split("\\s+");
				String pageName = parts[0].trim();
				long id = Long.parseLong(parts[1]);
				pageMapper.put(id, pageName);
			}
			reader.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void printMap() {
		for(long pageId: pageMapper.keySet()) {
			System.out.println(pageId + "--->" + pageMapper.get(pageId));
		}
	}

	// SInce we have used a key comparator which sorts in descending order, the data
	// to the
	// reducer comes in descending order of the key, i.e page rank.
	// Now, just emit the first 100 records since they are already sorted in
	// descending order.
	public void reduce(DoubleWritable pageRank, Iterable<LongWritable> pageIds, Context context)
			throws IOException, InterruptedException {

		double pagerank = pageRank.get();

		for (LongWritable pageIdWritable : pageIds) {
			long pageId = pageIdWritable.get();
			
			if(pageMapper.containsKey(pageId)) {
				if(num++ > 100) return;
				String pageName = pageMapper.get(pageId);
				context.write(new Text(pageName), new DoubleWritable(pagerank));
			}
		}

	}
}
