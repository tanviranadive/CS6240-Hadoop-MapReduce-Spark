package Graph.PageRank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
// Mapper emits col and cell for matrix M and a row cell for matrix R
public class MatrixMultiplicationMapper_1 extends Mapper<LongWritable, MatrixCell, LongWritable, MatrixCell> {

	public void map(LongWritable row, MatrixCell cell, Context context) throws IOException, InterruptedException {
		
		if(cell.type.equals("M")) {
			context.write(new LongWritable(cell.col), cell);
		} else if(cell.type.equals("R")) {
			context.write(row, cell);
		}		
	}
}

