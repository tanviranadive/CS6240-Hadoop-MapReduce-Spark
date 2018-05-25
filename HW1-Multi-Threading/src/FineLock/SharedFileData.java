package FineLock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// Use concurrent hashmap data structure for fine lock as accumulation data structure
public class SharedFileData {
	ConcurrentHashMap<String, Integer[]> chmap;
	
	public SharedFileData() {
		chmap = new ConcurrentHashMap<String, Integer[]>();
	}
	
	public void addInMap(String stationId, int tmax) {
		Integer[] t = chmap.get(stationId);
		t[0] = t[0] + tmax;
		t[1]++;
		chmap.put(stationId, t);
	}
	
}
