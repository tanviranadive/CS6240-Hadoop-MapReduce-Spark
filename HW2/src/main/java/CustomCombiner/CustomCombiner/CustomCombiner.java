package CustomCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// Combiner class aggregates tmin and tmax values for stationId from mapper
public class CustomCombiner extends Reducer<Text, StationData, Text, StationData>{
	
	public void reduce(Text key, Iterable<StationData> sdata, Context context) throws IOException, InterruptedException {
		StationData sd = new StationData();
		
		for(StationData s: sdata) {
			sd.tmin += s.tmin;
			sd.tminCount += s.tminCount;
			sd.tmax += s.tmax;
			sd.tmaxCount += s.tmaxCount;
		}
		
		context.write(key, sd);
	}

}
