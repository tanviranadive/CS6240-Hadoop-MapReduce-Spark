package InMapperCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer takes input (StationId, StationData) and emits (StationId, avgTmin, avgTmax)
public class InMapperCombinerReducer extends Reducer<Text, StationData, Text, Text>{

	public void reduce(Text key, Iterable<StationData> sdata, Context context) throws IOException, InterruptedException {
		double tminSum=0, tmaxSum = 0;
		long tminCount=0, tmaxCount = 0;
		
		// aggregate values for stationId
		for(StationData s: sdata) {
			tminSum += s.tmin;
			tminCount += s.tminCount;
			tmaxSum += s.tmax;
			tmaxCount += s.tmaxCount;
		}
		
		//calculate average
		double avgTmin = tminSum/tminCount;
		double avgTmax = tmaxSum/tmaxCount;
		
		Text value = new Text("- "+Double.toString(avgTmin)+ ", " + Double.toString(avgTmax));
		
		context.write(key, value);
	}
}
