package Graph.PageRank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper_2 extends Mapper<LongWritable, MatrixCell, LongWritable, MatrixCell> {

	public void map(LongWritable row, MatrixCell rCell, Context context) throws IOException, InterruptedException {
		
		context.write(row,  rCell);		
	}
}

