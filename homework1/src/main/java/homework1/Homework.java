package homework1;

import java.io.IOException;

/*
 * Entering class for the first Part of the Assignment
 */
public class Homework {

	public static void main(String[] args) throws IOException {
		System.out.println(args[0].toString());
		// Load records
		LoadDataStructure.loadRecords(args[0].toString());
		
		// Step 1. Sequential Average Calculation
		System.out.println("Part B");
		System.out.println("1. SEQ: Sequential version that calculates the average of the TMAX");
		Sequential s = new Sequential();
		s.executeSequentially();


		// Step 2. No Lock Average Calculation
		System.out.println();
		System.out.println();
		System.out.println("2. NO-LOCK: No locks or synchronization");
		NoLock nl = new NoLock();
		nl.executeNoLock();

		// Step 3. Coarse Lock Calculations
		System.out.println();
		System.out.println();
		System.out.println("3. COARSE-LOCK:single lock on the entire data structure");
		CoarseLock cl = new CoarseLock();
		cl.executeCoarseLock();

		// Step 4. No Lock Average Calculation
		System.out.println();
		System.out.println();
		System.out.println("3. FINE-LOCK:Locks only the accumulation value objects and not the whole data structure");
		FineLock fl = new FineLock();
		fl.executeFineLock();

		// Step 5. No Lock Average Calculation
		System.out.println();
		System.out.println();
		System.out.println("5. NO SHARING: Multi-threaded specially assigned to chunks and No-locks");
		NoSharing ns = new NoSharing();
		ns.executeNoSharing();
	}
}

