package InMapperCombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//Data structure StationData stores tminSum, tmaxSum, tminCount, tmaxCount
public class StationData implements WritableComparable<StationData>{

	public double tmin;
	public double tmax;
	public long tminCount;
	public long tmaxCount;
	
	public StationData() {
		this.tmin = 0;
		this.tmax = 0;
		this.tminCount = 0;
		this.tmaxCount = 0;
	}

	public void readFields(DataInput inp) throws IOException {
		
		
		this.tmin = inp.readDouble();
		this.tmax = inp.readDouble();
		this.tminCount = inp.readLong();
		this.tmaxCount = inp.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(tmin);
		out.writeDouble(tmax);
		out.writeLong(tminCount);
		out.writeLong(tminCount);
	}

	public int compareTo(StationData arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
