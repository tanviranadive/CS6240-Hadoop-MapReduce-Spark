package NoLock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import sharedData.*;


class NoLockThread extends Thread{
	
	// SharedData contains the data loaded from the input file
	private SharedData sharedData;
	// StationData contains the accumulation data structure
	private StationData stationData;
	private long startIndex;
	private long endIndex;
	
	public NoLockThread(SharedData sharedData, long startIndex, long endIndex, StationData stationData){
		this.sharedData = sharedData;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.stationData = stationData;		
	}
	
	public void run(){
		for(long i=startIndex;i<=endIndex;i++){
			String line = sharedData.data.get((int)i);
			
			// process only those records which contain TMAX field
			if(line.contains("TMAX")){
				String[] lineData = line.split(",");
				String stationId = lineData[0];
				int tmax = Integer.parseInt(lineData[3]);
				
				// no lock on the data structure, just add the data
				stationData.addData(stationId, tmax);
			}	
		}	
	}
}

//main program 
public class NoLockProgram{
	
	SharedData data;
	StationData stationData;
	long startIndex;
	long endIndex;
	
	// number of cores(threads)
	int cores = 4;
	int loadedDataSize;
	int chunkSize;
	ArrayList<NoLockThread> noLockThreads = new ArrayList<NoLockThread>();
	long startTime;
	long endTime;
	String outputFile = "src/output/noLockOutput.txt";
	long[] runningTime = new long[10];
	
	
	public NoLockProgram(ArrayList<String> loadedData) {
		data = new SharedData(loadedData);
		stationData = new StationData();
		startIndex = 0;
		endIndex = 0;
		loadedDataSize = loadedData.size();
		chunkSize = loadedDataSize/cores;
		
	}
	
	public void process() {
		System.out.println("NO LOCK");
	    System.out.println("--------------");
		
	    // repeat for 10 iterations
		for(int i=0;i<10;i++) {
			try {
				noLockThreads.clear();
				stationData.hmap.clear();
				startIndex = 0;
				endIndex = -1;
				
				// create threads and assign them data chunks to be worked on
				for(int j=0;j<cores;j++) {
					startIndex = endIndex+1;
					if(j==cores-1) {
						endIndex = loadedDataSize-1;
					}
					else
					{
					endIndex = startIndex + chunkSize;
					}
					
					noLockThreads.add(new NoLockThread(data,startIndex,endIndex,stationData));
				}
				
				
				startTime = System.currentTimeMillis();
				
				for(NoLockThread t: noLockThreads) {
					t.start();
				}
				
				for(NoLockThread t: noLockThreads) {
					t.join();
				}
				
				
				//average caluculation
				for (Map.Entry<String, Integer[]> entry : stationData.hmap.entrySet()) {
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
		}
		
		// calculate running times
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
	
	
	// writes data to output file
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
