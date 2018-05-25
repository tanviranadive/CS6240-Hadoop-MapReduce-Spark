package SecondarySort;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// Reducer takes in(StationYearData, StationData) and emits (StationId,[(year,avgTmin,avgTmax), (year,avgTmin, avgTmax),..])
public class SecondarySortReducer  extends Reducer<StationYearData, StationData, Text, Text>{
	
	// result string for one station
	StringBuffer result = new StringBuffer();
	//store previous year for comparison to check if year has changed to aggregate values
	String previousYear;
	double tminSum=0, tmaxSum = 0;
	long tminCount=0, tmaxCount = 0;
	
	public void reduce(StationYearData key, Iterable<StationData> sdata, Context context) throws IOException, InterruptedException {
		String stationId = key.stationId;
		String year = key.year;
		previousYear = "";
		double avgTmin=0;
		double avgTmax=0;
		tminSum=0;
		tmaxSum = 0;
		tminCount=0;
		tmaxCount=0;
		result.append("[");

		
		for(StationData s: sdata) {
			if(previousYear.equals("")) {
				System.out.println("first year");
				tminSum += s.tmin;
				tminCount += s.tminCount;
				tmaxSum += s.tmax;
				tmaxCount += s.tmaxCount;
				previousYear=year;
				
			}
			
			// aggregate values
			if(year.equals(previousYear)){
				System.out.println("year equal");
				tminSum += s.tmin;
				tminCount += s.tminCount;
				tmaxSum += s.tmax;
				tmaxCount += s.tmaxCount;
			}
			
			else {
				// year changed, compute average for previous year and append to result
				avgTmin = tminSum/tminCount;
				avgTmax = tmaxSum/tmaxCount;
				result.append("("+year+","+avgTmin+","+avgTmax+"),");
				tminSum=0;
				tmaxSum = 0;
				tminCount=0;
				tmaxCount=0;
				avgTmin = 0;
				avgTmax = 0;
				// set previous year to current year
				previousYear = year;
			}
		}
		
		// average for last record for the stationId
		avgTmin = tminSum/tminCount;
		avgTmax = tmaxSum/tmaxCount;
		result.append("("+year+","+avgTmin+","+avgTmax+")]");
		
		
		context.write(new Text(stationId),new Text(result.toString()));
	}

}
