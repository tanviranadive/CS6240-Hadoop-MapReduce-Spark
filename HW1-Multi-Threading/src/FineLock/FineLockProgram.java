package FineLock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import sharedData.SharedData;

class FineLockThread extends Thread{
	
	// SharedData contains the data loaded from the input file
	private SharedData sharedData;
	// StationData contains the accumulation data structure
	private SharedFileData lstationData;
	private long startIndex;
	private long endIndex;
	
	public FineLockThread(SharedData sharedData, long startIndex, long endIndex, SharedFileData stationData){
		this.sharedData = sharedData;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.lstationData = stationData;
	}
	
	public void run(){
		for(long i=startIndex;i<=endIndex;i++){
			String line = sharedData.data.get((int)i);
			
			if(line.contains("TMAX")){
				String[] lineData = line.split(",");
				String stationId = lineData[0];
				int tmax = Integer.parseInt(lineData[3]);
				Integer[] val = {tmax,1};				
				
				// Concurrent hashmap function for synchronization between threads
				if(lstationData.chmap.putIfAbsent(stationId, val)!=null){
					// lock only the record the thread is working on
					synchronized(lstationData.chmap.get(stationId)) {
						lstationData.addInMap(stationId, tmax);
					}
					
					//cost function
					fibonacciFunction(17);
				}	
			}	
		}	
	}
	
	// Cost function fibonacci to add delay in computation
	public int fibonacciFunction(int n) {
		if(n==0||n==1)
			return n;
		else 
			return fibonacciFunction(n-1) + fibonacciFunction(n-2);
	}
}

// main class
public class FineLockProgram {

	SharedData data;
	SharedFileData stationData;
	long startIndex;
	long endIndex;
	int cores = 4;
	int loadedDataSize;
	int chunkSize;
	ArrayList<FineLockThread> fineLockThreads = new ArrayList<FineLockThread>();
	long startTime;
	long endTime;
	String outputFile = "src/output/fineLockLock.txt";
	long[] runningTime = new long[10];
	
	
	public FineLockProgram(ArrayList<String> loadedData) {
		data = new SharedData(loadedData);
		stationData = new SharedFileData();
		startIndex = 0;
		endIndex = 0;
		loadedDataSize = loadedData.size();
		chunkSize = loadedDataSize/cores;
	}
	
	public void process() {
		System.out.println("\n FINE LOCK");
	    System.out.println("--------------");
		
	    // repeat process for 10 iterations
		for(int i=0;i<10;i++) {
			try {
				fineLockThreads.clear();
				stationData.chmap.clear();
				startIndex = 0;
				endIndex = -1;

				for(int j=0;j<cores;j++) {
					startIndex = endIndex+1;
					if(j==cores-1) {
						endIndex = loadedDataSize-1;
					}
					else
					{
					endIndex = startIndex + chunkSize;
					}
					
					fineLockThreads.add(new FineLockThread(data,startIndex,endIndex,stationData)); 	
				}
				
				startTime = System.currentTimeMillis();

				for(FineLockThread t: fineLockThreads) {
					t.start();
				}
				
				for(FineLockThread t: fineLockThreads) {
					t.join();
				}
				
				//average calculation
				
				for (Map.Entry<String, Integer[]> entry : stationData.chmap.entrySet()) {
				    String key = entry.getKey();
				    Integer[] value = entry.getValue();
				    double avg = (double)value[0]/value[1];
				    writeToFile(key + " "+Double.toString(avg));
				}
				endTime = System.currentTimeMillis();
				runningTime[i] = endTime-startTime;
				System.out.println("time required for iteration "+i+": "+(endTime-startTime));
			}
		
		catch(Exception e) {
			e.printStackTrace();
		}
		
			System.out.println("time required for iteration"+i+": "+ (endTime-startTime));
		}
		
		Arrays.sort(runningTime);
	    System.out.println("min running time: "+runningTime[0]);
	    System.out.println("max running time: "+runningTime[9]);
	    long runningSum = 0;
	    for(int i=0;i<9;i++) {
	    	runningSum = runningSum + runningTime[i];
	    }
	    
	    long runningAvg = runningSum/10;
	    System.out.println("Avg running time: "+runningAvg);
	}
	
	
	// write data to output file
	public void writeToFile(String s){
		try {
		    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true));
		    writer.append("\r\n");
		    writer.append(s);
		    writer.close();
	}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
