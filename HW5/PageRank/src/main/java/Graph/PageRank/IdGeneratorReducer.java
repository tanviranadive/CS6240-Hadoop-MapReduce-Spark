package Graph.PageRank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Generate page name id mapping
public class IdGeneratorReducer extends Reducer<Text, Text, Text, LongWritable> {

	private static int num;

	public void setup(Context context) {
		num = 0;
	}

	public void reduce(Text pageName, Iterable<Text> dummy, Context context) throws IOException, InterruptedException {

		LongWritable id = new LongWritable(num++);
		
		context.write(pageName, id);
	}
}
