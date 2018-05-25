package SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Secondary sort driver sets job config
public class SecondarySortDriver {
public static void main(String args[])throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Secondary Sort");
		job.setJarByClass(SecondarySortDriver.class);
	    job.setMapperClass(SecondarySortMapper.class);
	    job.setReducerClass(SecondarySortReducer.class);
	    job.setPartitionerClass(SecondarySortPartitioner.class);
	    job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
	    job.setNumReduceTasks(5);
	    job.setMapOutputKeyClass(StationYearData.class);
	    job.setMapOutputValueClass(StationData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
