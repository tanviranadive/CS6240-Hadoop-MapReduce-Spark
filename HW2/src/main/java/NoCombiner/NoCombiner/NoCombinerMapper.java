package NoCombiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper class emits StationId Text as key and StationData as value(type,temperature)
public class NoCombinerMapper extends Mapper<LongWritable, Text, Text, StationData>{
	public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException{
		
		// extract data from each line
		StationData sdata;
		String[] data = line.toString().split(",");
		String stationIdString = data[0].trim();
		Text stationId = new Text(stationIdString);
		
		Integer temperature = Integer.parseInt(data[3].trim());
		
		// store corresponding reading and count in StationData object
		if(data[2].trim().equals("TMAX")) {
			sdata = new StationData("TMAX", temperature);
			context.write(stationId, sdata);
		}
		
		else if(data[2].trim().equals("TMIN")) {
			sdata = new StationData("TMIN", temperature);
			context.write(stationId, sdata);
		}
	}
}
