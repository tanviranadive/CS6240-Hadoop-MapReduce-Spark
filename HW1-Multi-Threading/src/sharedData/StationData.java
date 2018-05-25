package sharedData;
import java.util.*;

// class to store the accumulation data structure shared by the threads
public class StationData {
	public HashMap<String, Integer[]> hmap;
	
	public StationData() {
		hmap = new HashMap<String, Integer[]>();
	}
	
	// add or update records
	public void addData(String stationId, int tmax) {
		if(hmap.containsKey(stationId)) {
		Integer[] val = hmap.get(stationId);	
		val[0] = val[0] + tmax;
		val[1]++;
		}
		
		else {
			Integer[] val = {tmax,1};
			hmap.put(stationId, val);
		}
		
		// add cost function delay
		fibonacciFunction(17);
	}
	
	// function for cost computation to add delay
	public int fibonacciFunction(int n) {
		if(n==0||n==1)
			return n;
		else 
			return fibonacciFunction(n-1) + fibonacciFunction(n-2);
	}

}
