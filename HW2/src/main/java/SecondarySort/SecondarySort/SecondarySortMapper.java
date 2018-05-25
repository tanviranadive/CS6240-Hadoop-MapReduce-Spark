package SecondarySort;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Mapper emits output (StationYearData, StationData)
public class SecondarySortMapper extends Mapper<LongWritable, Text, StationYearData, StationData>{
	
	// 
	public HashMap<StationYearData,StationData> hmap;
	
	public void setup(Context context) {
		hmap = new HashMap<StationYearData, StationData>();
	}
	
public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException {
		
	String[] data = line.toString().split(",");
	String stationIdString = data[0].trim();
	String year = data[1].trim().substring(0,4);
	StationYearData sy = new StationYearData();
	sy.stationId = stationIdString;
	sy.year = year;
	
	Double temperature = Double.parseDouble(data[3].trim());
	
	if(data[2].trim().equals("TMAX")) {
		if(hmap.containsKey(sy)) {
			StationData sd = hmap.get(sy);
			sd.tmax += temperature;
			sd.tmaxCount++;
		} else {
			StationData sd = new StationData();
			sd.tmax = temperature;
			sd.tmaxCount++;
			hmap.put(sy, sd);
		}
	} else if(data[2].trim().equals("TMIN")) {
		if(hmap.containsKey(sy)) {
			StationData sd = hmap.get(sy);
			sd.tmin += temperature;
			sd.tminCount++;
		} else {
			StationData sd = new StationData();
			sd.tmin = temperature;
			sd.tminCount++;
			hmap.put(sy, sd);
		}
	}
}

public void cleanup(Context context) throws IOException, InterruptedException {
	
	for(StationYearData sy: hmap.keySet()) {
		//System.out.println(sy.stationId+" "+sy.year+" "+ hmap.get(sy).tmax+" "+hmap.get(sy).tmin);
		context.write(sy, hmap.get(sy));
	}
}




}
