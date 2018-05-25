package NoCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Driver class to set job configurations
public class NoCombinerDriver {
	public static void main(String args[])throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "No Combiner");
		job.setJarByClass(NoCombinerDriver.class);
	    job.setMapperClass(NoCombinerMapper.class);
	    job.setReducerClass(NoCombinerReducer.class);
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
