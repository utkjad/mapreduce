/*
 * Utility functions for 
 * 1. Loading records
 * 2. Fibonacci 
 * 3. Printing Stats
 * 4. Printing Average
 */
package homework1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LoadDataStructure {
	static List<String> records = new ArrayList<String>();
	
	/*
	 * This method loads the input file and reads the records
	 * @returns List<String> records
	 */
	public static void loadRecords(String filePath) throws IOException {
		// Read file 
		FileReader fr =  new FileReader(filePath);
		BufferedReader br = new BufferedReader(fr);

		String line = null;
		// Read line by line and store it in Array list
		while ((line = br.readLine()) != null) {
			records.add(line);
		}
		fr.close();
	}
	
	/*
	 *  Takes the number and returns the fibonacci sum
	 */
	public static double fibonacci(int number) throws IOException {
		if (number == 0 || number == 1) {
			return 1;
		} else {
			return fibonacci(number - 2) + fibonacci(number - 1);
		}
	}
	
	/*
	 * Prints the minimum, maximum, and average of the input list
	 */
	public static void printStats(List<Double> listOfRecords){
		double sum = 0.0;
		for (double i:listOfRecords){
			sum += i;
		}
		System.out.println("Min - " + Collections.min(listOfRecords) +
				" Max - " + Collections.max(listOfRecords) +
				" Average - " + (sum/listOfRecords.size()));
	}
	
	/*
	 * Prints the StationID and its corresponding average TMAX value
	 * which is already stored in a Map.
	 */
	public static void printAverage(Map<String, double[]> accumulationRecord){
		accumulationRecord.forEach((key, value) -> System.out.println(key + " : " + value[1]));
	}

}
