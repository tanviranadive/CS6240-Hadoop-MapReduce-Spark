package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Grouping comparator groups on the basis of stationId only, not on year
public class SecondarySortGroupingComparator extends WritableComparator{
	protected SecondarySortGroupingComparator() {
		super(StationYearData.class, true);
	}
	
	
	public int compare(WritableComparable o1, WritableComparable o2) {
		StationYearData sy1 = (StationYearData) o1;
		StationYearData sy2 = (StationYearData) o2;
		return(sy1.stationId.compareTo(sy2.stationId));
		
	}

}
