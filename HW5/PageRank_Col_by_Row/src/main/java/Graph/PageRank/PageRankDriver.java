package Graph.PageRank;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * Driver Program
 */
public class PageRankDriver {
	
	private static String input;
	private static String output;
	static long totalPages;
	static float alpha = 0.15f;
	static int iteration = 0;
	static int loopCounter  = 10;
	static long sinkSum = 0;
	static int convergence = 10;

public static void main(String args[])throws Exception{
		
		input = args[0];
		output = args[1];
		
		try {
            File directory = new File(output);
            deleteDir(directory);
        } catch (Exception e) { e.printStackTrace(); } 
		
		Configuration conf = new Configuration();
		
		// mapping of page name to id
		totalPages = generateIds(conf);
		
		// generate initial matrices
		parseAndGenerateMatrices(conf);
		
		// Calculate contribution col-wise for M
		// Then aggregate contribution of each col in second job
		while(iteration++ <= convergence) {
			matrixMultiplicationJob_1(conf, iteration);
			matrixMultiplicationJob_2(conf, iteration);
		}			
		
		
		//find top k
		topKJob(conf, iteration);
		
	}

	public static void deleteDir(File dir) {
	    File[] files = dir.listFiles();
	
	    for (File myFile: files) {
	        if (myFile.isDirectory()) {  
	            deleteDir(myFile);
	        } 
	        myFile.delete();
	
	    }
	}

	// generate a mapping of page-name to id
	public static long generateIds(Configuration conf) throws Exception,IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankDriver.class);
	    job.setMapperClass(IdGeneratorMapper.class);
	    job.setReducerClass(IdGeneratorReducer.class);
	    job.setNumReduceTasks(1);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"/mapping"));
	   
	    boolean status = job.waitForCompletion(true);
	    
	    if(!status) throw new Exception("Job could not be completed");
	    
	    long totalNumPages = job.getCounters().findCounter("", "totalPagesCounter").getValue();
	    
	    return totalNumPages;
	}

	// Parse pages, generate M and R matrix
	public static void parseAndGenerateMatrices(Configuration conf) throws Exception,IOException, ClassNotFoundException, InterruptedException {
		
		conf.setLong("totalPages", totalPages);
		Job job = Job.getInstance(conf);
	
		job.setJarByClass(PageRankDriver.class);
	    job.setMapperClass(Bz2WikiParserMapper.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(MatrixCell.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(MatrixCell.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));	
	    FileOutputFormat.setOutputPath(job, new Path(output + "/initial-matrices"));
	    
	    Path directoryPath = new Path(output + "/mapping");
	    FileSystem fs = directoryPath.getFileSystem(conf);
	    FileStatus[] fileStatus = fs.listStatus(directoryPath);
	    for (FileStatus status : fileStatus) {
	    	String stringPath = status.getPath().toString();
	    	if(stringPath.contains("part"))
	    		job.addCacheFile(status.getPath().toUri());
	    }
	    
	    MultipleOutputs.addNamedOutput(job, "output", SequenceFileOutputFormat.class, LongWritable.class, MatrixCell.class);		

	    boolean status = job.waitForCompletion(true);
	    
	    if(!status) throw new Exception("Job could not be completed");
	    
	    sinkSum = job.getCounters().findCounter("", "SinkSum").getValue();
	    
	    System.out.println("SinkSum ->" + sinkSum);
	}
	
	// compute page-rank using matrix multiplication approach
	public static void matrixMultiplicationJob_1(Configuration conf, int iteration) throws Exception,IOException, ClassNotFoundException, InterruptedException {
		
		conf.setFloat("alpha", alpha);
		conf.setLong("totalPages", totalPages);
		conf.setLong("previousSinkSum", sinkSum);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PageRankDriver.class);
	    job.setMapperClass(MatrixMultiplicationMapper_1.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(MatrixCell.class);
	    job.setReducerClass(MatrixMultiplicationReducer_1.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(MatrixCell.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);	
	    FileOutputFormat.setOutputPath(job, new Path(output + "/intermediate-matrix-r-" + iteration));
	    
	    if(iteration <= 1) {
	    	Path matrixPaths = new Path(output + "/initial-matrices");
	    	FileInputFormat.addInputPath(job, matrixPaths);
	    } else {
	    	Path matrix_M_Path = new Path(output + "/initial-matrices");
		    Path matrix_R_Path = new Path(output + "/matrix-r-" + (iteration-1));
		    
		    MultipleInputs.addInputPath(job, matrix_M_Path, SequenceFileInputFormat.class, MatrixMultiplicationMapper_1.class);
		    MultipleInputs.addInputPath(job, matrix_R_Path, SequenceFileInputFormat.class, MatrixMultiplicationMapper_1.class);
	    }
	  
	    boolean status = job.waitForCompletion(true);
	    
	    if(!status) throw new Exception("Job could not be completed");
	    
	    sinkSum = job.getCounters().findCounter("", "SinkSum").getValue();
	    
	    System.out.println("SinkSum ->" + sinkSum);
	}
	
	// compute page-rank using matrix multiplication approach
		public static void matrixMultiplicationJob_2(Configuration conf, int iteration) throws Exception,IOException, ClassNotFoundException, InterruptedException {
			
			conf.setFloat("alpha", alpha);
			conf.setLong("totalPages", totalPages);
			conf.setLong("previousSinkSum", sinkSum);
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(PageRankDriver.class);
		    job.setMapperClass(MatrixMultiplicationMapper_2.class);
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(MatrixCell.class);
		    job.setReducerClass(MatrixMultiplicationReducer_2.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(MatrixCell.class);
		    job.setInputFormatClass(SequenceFileInputFormat.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);	

		    Path intermediate_matrix_R_Path = new Path(output + "/intermediate-matrix-r-" + iteration);
		    FileInputFormat.addInputPath(job, intermediate_matrix_R_Path);
		    
		    FileOutputFormat.setOutputPath(job, new Path(output + "/matrix-r-" + iteration));
		   
		    boolean status = job.waitForCompletion(true);
		    
		    if(!status) throw new Exception("Job could not be completed");
		    
		    sinkSum = job.getCounters().findCounter("", "SinkSum").getValue();
		    
		    System.out.println("SinkSum ->" + sinkSum);
		}
		
	// Last job, top-k job to calculate the top 100 pages in descending order of pageRank
	public static void topKJob(Configuration conf, int iteration) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankDriver.class);
	    job.setMapperClass(TopKMapper.class);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setSortComparatorClass(DoubleKeyComparator.class);
	    job.setNumReduceTasks(1);
	    job.setReducerClass(TopKReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(output + "/matrix-r-" + (iteration-1)));	
	    FileOutputFormat.setOutputPath(job, new Path(output+"/top-100 Pages"));
	    
	    Path directoryPath = new Path(output + "/mapping");
	    FileSystem fs = directoryPath.getFileSystem(conf);
	    FileStatus[] fileStatus = fs.listStatus(directoryPath);
	    for (FileStatus status : fileStatus) {
	    	String stringPath = status.getPath().toString();
	    	if(stringPath.contains("part"))
	    		job.addCacheFile(status.getPath().toUri());
	    }
	    
	    
	    boolean status = job.waitForCompletion(true);
	    
	    if(!status) { throw new Exception("Job could not be completed"); }
	}

}
