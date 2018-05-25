import java.util.*;
import java.io.*;

import NoLock.*;
import CoarseLock.*;
import NoSharing.*;
import FineLock.*;
import SequentialProgram.SequentialProgram;

// Main driver program for loading data and calling other program versions
public class Driver {

	public static void main(String args[]){
		String file = args[0];
		ArrayList<String> loadedData = fileLoader(file);
		
		// run the five versions of the program
		// comment out other versions for running only one particular
		SequentialProgram seq = new SequentialProgram(loadedData);
		seq.process();
		
		NoLockProgram noLock = new NoLockProgram(loadedData);
		noLock.process();
		
		CoarseLockProgram coarseLock = new CoarseLockProgram(loadedData);
		coarseLock.process();
		
		FineLockProgram fineLock = new FineLockProgram(loadedData);
		fineLock.process();
		
		NoSharingProgram noSharing = new NoSharingProgram(loadedData);
		noSharing.process();
	}
	
	// Loads input file into memory
	public static ArrayList<String> fileLoader(String file){
		ArrayList<String> data = new ArrayList<String>();

	try (BufferedReader br = new BufferedReader(new FileReader(file))) 
	{
	    String line;
	    while ((line = br.readLine()) != null) 
	    {
	       data.add(line);
	    }
	    
	    br.close();
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	
	return data;
	}
}
