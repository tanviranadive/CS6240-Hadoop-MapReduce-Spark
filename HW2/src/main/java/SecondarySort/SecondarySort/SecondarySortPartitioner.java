package SecondarySort;

import org.apache.hadoop.mapreduce.Partitioner;

// Partitioner partitions on the basis of stationId
public class SecondarySortPartitioner extends Partitioner<StationYearData, StationData>{

	@Override
	public int getPartition(StationYearData sy, StationData sdata, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(sy.stationId.hashCode()%numPartitions);
	}

}
