package Graph.PageRank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends Mapper<LongWritable, MatrixCell, LongWritable, MatrixCell> {

	public void map(LongWritable row, MatrixCell cell, Context context) throws IOException, InterruptedException {
		
		// emit the row cell from matrix M
		if(cell.type.equals("M")) {
			context.write(row, cell);
		}			
	}
}

