package Graph.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// store type of matrix ("M" or "R", row number, col number or NumOutlinks and the PageRank value
public class MatrixCell implements Writable {
	public String type;
	public long row;
	// in M matrix column value is used to store column Number
	// in R matrix column value is used to store no. of out-links
	public long col; 
	public double value;
	
	public MatrixCell() {
		type = "";
		row = -1;
		col = -1;
		value = -1;
	}
	
	public MatrixCell(String type, long row, long col, double value) {
		this.type = type;
		this.row = row;
		this.col = col;
		this.value = value;
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeUTF(type);
		out.writeLong(row);
		out.writeLong(col);
		out.writeDouble(value);
	}
	  
	public void readFields(DataInput in) throws IOException{
	 	this.type = in.readUTF();
		this.row = in.readLong();
	 	this.col = in.readLong();
	 	this.value = in.readDouble();
	}	
}