package CustomCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Driver class sets job confguration
public class CustomCombinerDriver {
public static void main(String args[])throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Custom Combiner");
		job.setJarByClass(CustomCombinerDriver.class);
	    job.setMapperClass(CustomCombinerMapper.class);
	    job.setCombinerClass(CustomCombiner.class);
	    job.setReducerClass(CustomCombinerReducer.class);
	    job.setNumReduceTasks(5);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(StationData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
