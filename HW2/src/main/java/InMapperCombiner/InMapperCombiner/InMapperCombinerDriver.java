package InMapperCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Driver class sets job confguration
public class InMapperCombinerDriver {
public static void main(String args[])throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "In Mapper Combiner");
		job.setJarByClass(InMapperCombinerDriver.class);
	    job.setMapperClass(InMapperCombinerMapper.class);
	    job.setReducerClass(InMapperCombinerReducer.class);
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
