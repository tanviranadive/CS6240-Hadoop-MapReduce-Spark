package NoSharing;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import sharedData.*;

	
	class NoSharingThread extends Thread{
		
		// SharedData contains the data loaded from the input file
		private SharedData sharedData;
		
		// Accumulation data structure local for each thread
		private HashMap<String, Integer[]> stationData;
		
		// start and end indices of data in SharedData structure for partitioning
		private long startIndex;
		private long endIndex;
		
		// initializing variables with constructor when new thread is created
		public NoSharingThread(SharedData sharedData, long startIndex, long endIndex){
			this.sharedData = sharedData;
			this.startIndex = startIndex;
			this.endIndex = endIndex;
			this.stationData = new HashMap<String, Integer[]>();
			
		}
		
		// When each thread starts, process records from data extracted from file
		public void run(){
			for(long i=startIndex;i<=endIndex;i++){
				String line = sharedData.data.get((int)i);
				
				// only process records which contain TMAX field
				if(line.contains("TMAX")){
					String[] lineData = line.split(",");
					String stationId = lineData[0];
					int tmax = Integer.parseInt(lineData[3]);
					
					//add or update records based on stationId
					if(stationData.containsKey(stationId)){
						Integer[] t = stationData.get(stationId);
						t[0] = t[0] + tmax;
						t[1]++;
						stationData.put(stationId, t);
					}
					else{
						Integer[] t = {tmax,1};
						stationData.put(stationId, t);
					}
				}	
			}	
		}
		
		// returns local accumulation data of each thread
		public HashMap<String, Integer[]> getLocalData(){
			return stationData;
		}
	}

	//main program
	public class NoSharingProgram{
		
		SharedData data;
		HashMap<String, Integer[]> hmap;
		long startIndex,endIndex;
		// number of cores used (threads)
		int cores = 4;
		// size of data obtained from file loading
		int loadedDataSize;
		// size of each partition of data to be given to each thread
		int chunkSize;
		String outputFile = "src/output/noSharingOutput.txt";
		ArrayList<NoSharingThread> noSharingThreads = new ArrayList<NoSharingThread>();
		long startTime,endTime;
		long[] runningTime = new long[10];
		
		
		public NoSharingProgram(ArrayList<String> loadedData) {
			data = new SharedData(loadedData);
			hmap = new HashMap<String, Integer[]>(); 
			startIndex = 0;
			endIndex = 0;
			loadedDataSize = loadedData.size();
			// allocate chunk size based on file size and number of threads
			chunkSize = loadedDataSize/cores;
			
		}
		
		public void process() {
			System.out.println("\n NO SHARING");
		    System.out.println("--------------");
			
			for(int i=0;i<10;i++) {
				try {
					
					noSharingThreads.clear();
					hmap.clear();
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
						
						noSharingThreads.add(new NoSharingThread(data,startIndex,endIndex)); 
						
					}
					
					startTime = System.currentTimeMillis();

					for(NoSharingThread t: noSharingThreads) {
						t.start();
					}
					
					for(NoSharingThread t: noSharingThreads) {
						t.join();
					}
					
					for(NoSharingThread t: noSharingThreads) {
						HashMap<String, Integer[]> localData = t.getLocalData();
						
						for(Map.Entry<String, Integer[]> entry:localData.entrySet()) {
							String stationId = entry.getKey();
							int tmax = entry.getValue()[0];
							int count  = entry.getValue()[1];
							if(hmap.containsKey(stationId)) {
								Integer[] value = hmap.get(stationId);
								value[0] = value[0] + tmax;
								value[1] = value[1] + count;
							}
							else {
								Integer[] value = {tmax, count};
								hmap.put(stationId,value);
							}
							
							// call cost function
							fibonacciFunction(17);
						}
					}
					
					// calculate the average for each record
					for (Map.Entry<String, Integer[]> entry : hmap.entrySet()) {
					    String key = entry.getKey();
					    Integer[] value = entry.getValue();
					    double avg = (double)value[0]/value[1];
					    writeToFile(key  + " "+ Double.toString(avg));
					}
					
					endTime = System.currentTimeMillis();
					runningTime[i] = endTime - startTime;
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
		
		
		// cost function to calculate fibonacci
		public int fibonacciFunction(int n) {
			if(n==0||n==1)
				return n;
			else 
				return fibonacciFunction(n-1) + fibonacciFunction(n-2);
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



