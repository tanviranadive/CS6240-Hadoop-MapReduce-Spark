package CustomCombiner;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper class emits (StationId, StationData)
public class CustomCombinerMapper extends Mapper<LongWritable, Text, Text, StationData>{
	
public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException{
		
	StationData sdata;
	String[] data = line.toString().split(",");
	String stationIdString = data[0].trim();
	Text stationId = new Text(stationIdString);
	
	Integer temperature = Integer.parseInt(data[3].trim());
	
	// store tmin and tmax values depending upon type and emits (stationId, StationData)
	if(data[2].trim().equals("TMAX")) {
		sdata = new StationData();
		sdata.tmax = temperature;
		sdata.tmaxCount++;
		context.write(stationId, sdata);
	} else if(data[2].trim().equals("TMIN")) {
		sdata = new StationData();
		sdata.tmin = temperature;
		sdata.tminCount++;
		context.write(stationId, sdata);
	}
	}


}
