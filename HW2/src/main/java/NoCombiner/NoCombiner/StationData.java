package NoCombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// StationData stores type "TMIN" or "TMAX"
public class StationData implements WritableComparable<StationData>{

	String type;
	double value;
	
	public StationData() {
		this.type = "";
		this.value = 0;
	}
	
	public StationData(String type, double value) {
		this.type = type;
		this.value = value;
	}

	public void readFields(DataInput inp) throws IOException {
		Text type = new Text();
		type.readFields(inp);
		this.type = type.toString();
		
		this.value = inp.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		Text typ = new Text(this.type);
		typ.write(out);
		out.writeDouble(value);
	}

	public int compareTo(StationData arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
}
