package InMapperCombiner;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Mapper class emits (StationId, StationData) after aggregating
public class InMapperCombinerMapper extends Mapper<LongWritable, Text, Text, StationData>{

	//HashMap to store key,value pair for local aggregation
	private HashMap<String,StationData> hmap;
	
	// setup method called before processing. defines new HashMap
	public void setup(Context context) {
		hmap = new HashMap<String, StationData>();
	}
	
public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException{
		
		StationData sdata;
		String[] data = line.toString().split(",");
		String stationIdString = data[0].trim();
		Text stationId = new Text(stationIdString);
		
		Integer temperature = Integer.parseInt(data[3].trim());
		
		// If key already present, aggregate values and store in hashmap
		if(data[2].trim().equals("TMAX")) {
			if(hmap.containsKey(stationId)) {
				StationData sd = hmap.get(stationId);
				sd.tmax += temperature;
				sd.tmaxCount++;
			} else {
				StationData sd = new StationData();
				sd.tmax = temperature;
				sd.tmaxCount++;
				hmap.put(stationIdString, sd);
			}
		} else if(data[2].trim().equals("TMIN")) {
			if(hmap.containsKey(stationId)) {
				StationData sd = hmap.get(stationId);
				sd.tmin += temperature;
				sd.tminCount++;
			} else {
				StationData sd = new StationData();
				sd.tmin = temperature;
				sd.tminCount++;
				hmap.put(stationIdString, sd);
			}
		}
	}

// cleanup method called after processing is completed for map. Emits (stationId, StationData)
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		for(String stationId: hmap.keySet()) {
			context.write(new Text(stationId), hmap.get(stationId));
		}
	}

}
