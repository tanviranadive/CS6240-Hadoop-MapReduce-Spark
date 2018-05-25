package Graph.PageRank;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
// aggregate page rank contributions of all inlinks
public class MatrixMultiplicationReducer_2 extends Reducer<LongWritable, MatrixCell, LongWritable, MatrixCell> {
	
	static float alpha;
	static long totalPages;
	static long previousSinkSum;
	static double newSinkSum;
	
	public void setup(Context context) throws IOException {
		alpha = context.getConfiguration().getFloat("alpha", -10);
		totalPages = context.getConfiguration().getLong("totalPages", -10);
		previousSinkSum = context.getConfiguration().getLong("previousSinkSum", -10);
		newSinkSum = 0.0d;
	}

	public void reduce(LongWritable row, Iterable<MatrixCell> rCells, Context context) throws IOException, InterruptedException {
		double totalPageRank = 0.0d;
		long numOutLinks = 0;
		
		for(MatrixCell rCell: rCells) {
			numOutLinks = rCell.col;
			totalPageRank += rCell.value;
		}
		
		double teleportationFactor = (double) alpha / totalPages;
		totalPageRank = teleportationFactor + (double) (1 - alpha) * totalPageRank;
		totalPageRank += (1 - alpha) * previousSinkSum / (1000000000 * totalPages);

		MatrixCell newRCell = new MatrixCell("R", row.get(), numOutLinks, totalPageRank);
	
		if(newRCell.col == 0) { newSinkSum += totalPageRank; }

		context.write(row, newRCell);
	}
	
	public void cleanup(Context context) {
		long totalSinkSum = (long) (newSinkSum * 1000000000);
		context.getCounter("", "SinkSum").increment(totalSinkSum);
	}
}

