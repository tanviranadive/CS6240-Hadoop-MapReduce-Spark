package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// Data structure stores StationId and year
public class StationYearData implements WritableComparable<StationYearData>{
	
	public String stationId;
	public String year;
	
	public StationYearData() {
		
	}
	

public void readFields(DataInput inp) throws IOException {
		this.stationId = inp.readUTF();
		this.year = inp.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stationId);
		out.writeUTF(this.year);
	}

	// override the compareTo method to for key comparator to sort on the basis of (stationId,year)
	// in ascending order
	@Override
	public int compareTo(StationYearData sy) {

		if (this.stationId.equals(sy.stationId))
			return (this.year.compareTo(sy.year));
		else
			return (this.stationId.compareTo(sy.stationId));
			
	}	

	public Object getStationId() {
		// TODO Auto-generated method stub
		return this.stationId;
	}
	
	
	
	// override hashcode method for key comparison
	@Override
	public int hashCode() {
		return (this.stationId.toString() + "," + this.year.toString()).hashCode();
}
	
	// override equals method to compare two (stationId,year) objects
	@Override
	public boolean equals(Object obj) {
		StationYearData sy = (StationYearData) obj;
		return (this.stationId.equals(sy.stationId) && this.year
				.equals(sy.year));
}
	
	@Override
	public String toString() {
		return (this.stationId + "," + this.year);
}
	
	
}
