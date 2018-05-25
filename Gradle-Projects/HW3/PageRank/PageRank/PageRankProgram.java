package PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Driver Program
 */
public class PageRankProgram {
	
	private static String input;
	private static String output;
	static long totalPages;
	static float alpha = 0.15f;
	static int iteration;
	static long initialSinkSum;
	static int loopCounter  = 10;

public static void main(String args[])throws Exception{
		
		input = args[0];
		output = args[1];
		
		Configuration conf = new Configuration();
		// Get the total number pages from first job which does the parsing of input files
		totalPages = inputParser(conf);
		
		// Second job to set the initial page rank and data structure
		//setPageRankData(conf);
		
		// Page Rank iterations. Pass the sink sum from previous iteration to the next
		iteration = 0;
		initialSinkSum = 0;
		long sinkSum = initialSinkSum;
		while(iteration<=loopCounter) {
			sinkSum = pageRankIteration(conf, iteration, sinkSum);
			iteration++;
		}
		
		// Last job to get the top 100 results
		topKJob(conf, iteration);
		
	}


	// First job, parsing input using bz2parser and return total number of pages found
	public static long inputParser(Configuration conf) throws Exception,IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf,"PreProcessing");
		job.setJarByClass(PageRankProgram.class);
	    job.setMapperClass(Bz2WikiParser.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PageData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(PageData.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"data-0"));
	   
	    boolean status = job.waitForCompletion(true);
	    if(!status) {
	    	throw new Exception("Job could not be completed");
	    }
	    
	    long totalNumPages = job.getCounters().findCounter("", "totalPagesCounter").getValue();
	    return totalNumPages;
	    
	}
	
	// Second job, set intial page rank and data structure for input of subsequent jobs. Map only job
	public static void setPageRankData(Configuration conf) throws Exception,IOException, ClassNotFoundException, InterruptedException {
		
		conf.setLong("totalPages", totalPages);
		Job job = new Job(conf,"InitialPageRankData");
		job.setJarByClass(PageRankProgram.class);
	    job.setMapperClass(InitialPageRankMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PageData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(PageData.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(output+"data"));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"data-0"));
	    boolean status = job.waitForCompletion(true);
	    if(!status) {
	    	throw new Exception("Job could not be completed");
	    }
	    
	    initialSinkSum = job.getCounters().findCounter("", "initialSinkSum").getValue();
	}
	
	// Third job, page rank iterations job. Map-reduce jobs called for 10 iterations. 
	// Returns the sink sum obtained in the iteration to be passed onto next iteration
	public static long pageRankIteration(Configuration conf, int i, long sinkSum) throws Exception {
		conf.setLong("totalPages", totalPages);
		conf.setFloat("alpha",alpha);
		conf.setInt("iteration", i);
		conf.setLong("previousSinkSum", sinkSum);
		long newSinkSum;
		
		Job job = new Job(conf,"PageRankIteration");
		job.setJarByClass(PageRankProgram.class);
	    job.setMapperClass(PageRankMapper.class);
	    job.setReducerClass(PageRankReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PageData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(PageData.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(output+"data-"+(i)));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"data-"+(i+1)));
	    boolean status = job.waitForCompletion(true);
	    if(!status) {
	    	throw new Exception("Job could not be completed");
	    }
	    
	    // sink sum for iteration i
	    newSinkSum = job.getCounters().findCounter("", "newSinkSum"+i).getValue();
	    return newSinkSum;
	}
		
	// Last job, top-k job to calculate the top 100 pages in descending order of pageRank
	public static void topKJob(Configuration conf, int i) throws Exception {
		Job job = new Job(conf,"LastIteration");
		job.setJarByClass(PageRankProgram.class);
	    job.setMapperClass(TopKMapper.class);
	    job.setReducerClass(TopKReducer.class);
	    // set number of reduce tasks 1 to send all data to single reducer
	    job.setNumReduceTasks(1);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(output+"data-"+(i)));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"top-100 Pages"));
	    boolean status = job.waitForCompletion(true);
	    if(!status) {
	    	throw new Exception("Job could not be completed");
	    }
	}

}
