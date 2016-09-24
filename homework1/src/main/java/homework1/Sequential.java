package homework1;

import java.io.IOException;
import java.util.*;

/*
 * Update Station ID and average TMAX using Sequential Method
 */
public class Sequential {
	double startTime;
	double endTime;
	HashMap<String, double []> accumulationRecord = new HashMap<String, double[]>();
	List<String> records = new ArrayList<String>();
	
	public Sequential() throws IOException{
		// Get records in List format
		this.records = LoadDataStructure.records;
	}
	
	
	public void calculateAverage(Boolean withFibonacciHuh) throws IOException{
		// Get Start time
		this.startTime = System.currentTimeMillis();
		
	    for (String record: this.records){
	    	if (record.contains("TMAX")){
	    		String stationID = new String(record.split(",")[0]);
	    		Double tmaxCurrentRecord = new Double(Double.parseDouble(record.split(",")[3]));
	    		
	    		if (this.accumulationRecord.containsKey(stationID)){
	    			double [] tempValues = new double[2];
	    			tempValues = this.accumulationRecord.get(stationID);
	    			
	    			double tempCount =  tempValues[0];
	    			double averageSoFar =  tempValues[1];
	    			
	    			// Now, update count and Average
	    			double sumSoFar = tempCount * averageSoFar;
	    			double newSum = sumSoFar + tmaxCurrentRecord;
	    			
	    			tempValues[0] = tempCount + 1;
	    			tempValues[1] = newSum/ tempValues[0]; // New Average
	    			
	    			// Calculate the Fibonacci (part C)
	    			if (withFibonacciHuh) {
		    			LoadDataStructure.fibonacci(17);
					}
	    			
	    			this.accumulationRecord.put(stationID, tempValues);
	    			
	    		}else{
	    			double [] tempValues = new double[2];
	    			// Add Count, and average
	    			tempValues[0] = 1;
	    			tempValues[1] = tmaxCurrentRecord; // Average will also be same here
	    			
	    			if (withFibonacciHuh) {
		    			LoadDataStructure.fibonacci(17);
					}
	    			
	    			// Put in Data Structure
	    			this.accumulationRecord.put(stationID, tempValues);
	    		}
	    	}
	    }
	    
	    // Get end time
	    this.endTime = System.currentTimeMillis();
	}
	
	/*
	 * Entry level function for Sequential Run
	 */
	public void executeSequentially() throws IOException{
		List<Double> runningTime = new ArrayList<Double>();
		
		System.out.println("\nWithout Fibonacci 10 times\n");
		for(int i=0;i<10; i++){
			System.out.print("Run #" + i);
			this.calculateAverage(false);
			System.out.print(" Time -> " + (this.endTime - this.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(this.endTime - this.startTime);
		}
		LoadDataStructure.printStats(runningTime);
		
		
		runningTime = new ArrayList<Double>();
		System.out.println("\nWith Fibonacci 10 times\n");
		for(int i=0;i<10; i++){
			System.out.print("Run #" + i);
			this.calculateAverage(true);
			System.out.print(" Time -> " + (this.endTime - this.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(this.endTime - this.startTime);
		}
		LoadDataStructure.printStats(runningTime);
	}

}
