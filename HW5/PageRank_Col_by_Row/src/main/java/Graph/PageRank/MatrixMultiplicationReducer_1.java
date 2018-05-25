package Graph.PageRank;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
// emit R with updated values
public class MatrixMultiplicationReducer_1 extends Reducer<LongWritable, MatrixCell, LongWritable, MatrixCell> {
	
	public void setup(Context context) throws IOException {}

	
	public void reduce(LongWritable col, Iterable<MatrixCell> cells, Context context) throws IOException, InterruptedException {
		
		MatrixCell rCell = null;
		ArrayList<MatrixCell> mCells = new ArrayList<MatrixCell>();
		
		for(MatrixCell cell: cells) {
			if(cell.type.equals("M"))
				mCells.add(cell.clone());
			else if(cell.type.equals("R"))
				rCell = cell.clone();
		}
		
		if(rCell.col != 0) {
			for(MatrixCell mCell : mCells) {
				LongWritable row = new LongWritable(mCell.row);
				double pageRankContribution = rCell.value/rCell.col;
				MatrixCell newRCellWithContribution = new MatrixCell("R", mCell.row, rCell.col, pageRankContribution);
				context.write(row, newRCellWithContribution);
			}
		}
	}
}

