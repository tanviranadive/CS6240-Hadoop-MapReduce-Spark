package NoCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer takes (StationId, StationData) as input and writes (StationId, AvgTmin, avgTmax) to output
public class NoCombinerReducer extends Reducer<Text, StationData, Text, Text>{

	public void reduce(Text key, Iterable<StationData> sdata, Context context) throws IOException, InterruptedException {
		double tminSum=0, tmaxSum = 0;
		int tminCount=0, tmaxCount = 0;
		
		// aggregate tminSum and tmaxSum for stationId key
		for(StationData s: sdata) {
			if(s.type.equals("TMIN")) {
				tminSum = tminSum + s.value;
				tminCount++;
			}
			
			else if(s.type.equals("TMAX")) {
				tmaxSum = tmaxSum + s.value;
				tmaxCount++;
			}
			}
		
		// calculate average
		double avgTmin = tminSum/tminCount;
		double avgTmax = tmaxSum/tmaxCount;
		
		Text value = new Text("- "+Double.toString(avgTmin)+ ", " + Double.toString(avgTmax));
		
		context.write(key, value);
		
	}
}
