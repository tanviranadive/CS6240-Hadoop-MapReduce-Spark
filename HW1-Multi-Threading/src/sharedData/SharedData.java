package sharedData;

import java.util.*;

// class for storing the contents loaded from file in a shared data
public class SharedData {

	public ArrayList<String> data = new ArrayList<String>();
	
	public SharedData(ArrayList<String> loadedData) {
		this.data = loadedData;
	}
	
}
