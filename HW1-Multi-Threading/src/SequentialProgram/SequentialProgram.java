package SequentialProgram;

import java.io.*;
import java.util.*;
import sharedData.*;


public class SequentialProgram {

	// contains the data loaded from the input file
	ArrayList<String> data;
	SharedData loadedData;
	StationData stationData;
	private long startTime,endTime;
	public String outputFile = "src/output/sequentialOutput.txt";
	private long[] runningTime = new long[10];
	public SequentialProgram(ArrayList<String> loadedData){
		stationData = new StationData();
		this.data = loadedData;		
	}
	
	public void process(){
		
		 System.out.println("SEQUENTIAL");
		 System.out.println("--------------");
		    
		// repeat for 10 iterations
		for(int i=0;i<10;i++) {
		stationData.hmap.clear();	
		startTime = System.currentTimeMillis();
		for(String line : data){
			
			// process only records containing TMAX
			if(line.contains("TMAX")){
				String[] lineData = line.split(",");
				String stationId = lineData[0];
				int tmax = Integer.parseInt(lineData[3]);
				stationData.addData(stationId, tmax);
			
				}
			}
		
		// average calculation
		for (Map.Entry<String, Integer[]> entry : stationData.hmap.entrySet()) {
		    String key = entry.getKey();
		    Integer[] value = entry.getValue();
		    int sum = value[0];
		    int count = value[1];
		    double avg = (double)sum/count;
		    writeToFile(key + " "+ Double.toString(avg));
		}
		
		endTime = System.currentTimeMillis();
		runningTime[i] = endTime-startTime;
		System.out.println("time required for iteration "+i+": "+(endTime-startTime));

		}
		
		// calculate running time
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


