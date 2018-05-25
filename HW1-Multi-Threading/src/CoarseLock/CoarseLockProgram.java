package CoarseLock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import sharedData.*;


class CoarseLockThread extends Thread{
	
	// SharedData contains the data loaded from the input file
	private SharedData sharedData;
	
	// StationData contains the accumulation data structure
	private StationData stationData;
	private long startIndex;
	private long endIndex;
	
	public CoarseLockThread(SharedData sharedData, long startIndex, long endIndex, StationData stationData){
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
				
				// lock the entire accumulation data structure to process each record
				synchronized(stationData.hmap) {
					stationData.addData(stationId,tmax);
				}
			}	
		}	
	}
}

// main class
public class CoarseLockProgram{
	
	SharedData data;
	StationData stationData;
	long startIndex;
	long endIndex;
	int cores = 4;
	int loadedDataSize;
	int chunkSize;
	String outputFile = "src/output/coarseLockOutput.txt";
	ArrayList<CoarseLockThread> coarseLockThreads = new ArrayList<CoarseLockThread>();
	long startTime;
	long endTime;
	long[] runningTime = new long[10];
	
	
	public CoarseLockProgram(ArrayList<String> loadedData) {
		data = new SharedData(loadedData);
		stationData = new StationData();
		startIndex = 0;
		endIndex = 0;
		loadedDataSize = loadedData.size();
		chunkSize = loadedDataSize/cores;
	}
	
	public void process() {
		
		System.out.println("\n COARSE LOCK");
	    System.out.println("--------------");
		
		for(int i=0;i<10;i++) {
			try {
				coarseLockThreads.clear();
				stationData.hmap.clear();
				startIndex = 0;
				endIndex = -1;

				// for each thread, provide a start and endIndex 
				// for the chunk of data it is supposed to work on
				for(int j=0;j<cores;j++) {
					startIndex = endIndex+1;
					if(j==cores-1) {
						endIndex = loadedDataSize-1;
					}
					else
					{
					endIndex = startIndex + chunkSize;
					}
					
					coarseLockThreads.add(new CoarseLockThread(data,startIndex,endIndex,stationData));
				}
				
				startTime = System.currentTimeMillis();

				for(CoarseLockThread t: coarseLockThreads) {
					t.start();
				}
				
				for(CoarseLockThread t: coarseLockThreads) {
					t.join();
				}
				
				
				//calculate average for each record
				for (Map.Entry<String, Integer[]> entry : stationData.hmap.entrySet()) {
				    String key = entry.getKey();
				    Integer[] value = entry.getValue();
				    double avg = (double)value[0]/value[1];
				    writeToFile(key + " "+ Double.toString(avg));
				}
				
				endTime = System.currentTimeMillis();
				runningTime[i] = endTime-startTime;
				System.out.println("time required for iteration"+i+": "+ (endTime-startTime));
			}

			catch(Exception e) {
			e.printStackTrace();
			}	
		}
		
		// running times calculation
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
	
	
	// this function writes data to output file
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
