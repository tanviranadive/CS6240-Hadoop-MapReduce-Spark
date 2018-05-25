package Graph.PageRank;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;


public class MatrixMultiplicationReducer extends Reducer<LongWritable, MatrixCell, LongWritable, MatrixCell> {

	static float alpha;
	static long totalPages;
	static long previousSinkSum;
	static double newSinkSum;
	static Map<Long, MatrixCell> matrixR;


	public void setup(Context context) throws IOException {
		alpha = context.getConfiguration().getFloat("alpha", -10);
		totalPages = context.getConfiguration().getLong("totalPages", -10);
		previousSinkSum = context.getConfiguration().getLong("previousSinkSum", -10);
		newSinkSum = 0.0d;
		
		// load R from distributed cache into a hashmap
		matrixR = new HashMap<Long, MatrixCell>();
		loadMatrixR(context);
	}
	
	// load matrix R from distributed cache
	public void loadMatrixR(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();
		if(cacheFiles != null && cacheFiles.length > 0) {
			try {					
				for(URI cacheFile: cacheFiles) {
					readFile(cacheFile, context.getConfiguration());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void readFile(URI cacheFile, Configuration conf) {
		try {
			SequenceFile.Reader seqFileReader = new Reader(conf, Reader.file(new Path(cacheFile)));			
			LongWritable row = new LongWritable();
			MatrixCell rCell = new MatrixCell();
			while (seqFileReader.next(row, rCell)) {
				MatrixCell newRCell = new MatrixCell(rCell.type, rCell.row, rCell.col, rCell.value);
				matrixR.put(row.get(), newRCell);					
			}
			seqFileReader.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void printMap() {
		for(Long row: matrixR.keySet()) {
			System.out.println(row + "--->" + matrixR.get(row).col + " -> " + matrixR.get(row).value);
		}
	}
	
	// for each row, calculate total page rank contributed by columns
	public void reduce(LongWritable row, Iterable<MatrixCell> mCells, Context context) throws IOException, InterruptedException {
		
		double totalPageRank = 0.0d;
		
		for(MatrixCell mCell: mCells) {
			if(matrixR.containsKey(mCell.col)) {
				MatrixCell rCell = matrixR.get(mCell.col);
				if(rCell.col != 0)
					totalPageRank += rCell.value/rCell.col;
			}
		}
		
		double teleportationFactor = (double) alpha / totalPages;
		totalPageRank = teleportationFactor + (double) (1 - alpha) * totalPageRank;
		totalPageRank += (1 - alpha) * previousSinkSum / (1000000000 * totalPages);

		MatrixCell oldRCell = matrixR.get(row.get());
		oldRCell.value = totalPageRank;
	
		
		if(oldRCell.col == 0) { newSinkSum += totalPageRank; }

		context.write(row, oldRCell);
	}

	
	public void cleanup(Context context) {
		long totalSinkSum = (long) (newSinkSum * 1000000000);
		context.getCounter("", "SinkSum").increment(totalSinkSum);
	}
}

