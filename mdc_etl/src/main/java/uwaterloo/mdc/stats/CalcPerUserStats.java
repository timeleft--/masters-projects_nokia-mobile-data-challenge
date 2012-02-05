package uwaterloo.mdc.stats;

import java.io.File;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;

public class CalcPerUserStats {

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "P:\\mdc-datasets\\stats";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO dataRoot from args
		new CalcPerUserStats().count();
	}

	Map<String, Boolean> freqWriterLocks = Collections
			.synchronizedMap(new HashMap<String, Boolean>());
	
	Map<String, Writer> freqWriterMap = Collections
			.synchronizedMap(new HashMap<String, Writer>());

	private void count() throws Exception {
		//To make sure the class is loaded
		System.out.println(PerfMon.asString());
		
		CallableOperationFactory<Frequency> factory = new CallableOperationFactory<Frequency>();
		
		try {
			// TODO: ends with time or equals start is the time
			ExecutorService exec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);

			File dataRootFile = FileUtils.getFile(dataRoot);
			for (File userDir : dataRootFile.listFiles()) {
				for (File dataFile : userDir.listFiles()) {
					//"accel.csv".equals(dataFile.getName()) ||
					if ("distance_matrix.csv".equals(dataFile.getName())) {
						continue;
					}

					PerUserDistinctValues distinctValues = (PerUserDistinctValues) factory.createOperation(PerUserDistinctValues.class,
							this, dataFile, outPath);

					// Future<HashMap<String, Frequency>> resultFuture =
					exec.submit(distinctValues);

				}
			}
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			exec.shutdown();
			// Wait until all threads are finish
			while (!exec.isTerminated()) {
				Thread.sleep(5000);
				System.out.println(PerfMon.asString());
			}
		} finally {
			long delta = System.currentTimeMillis();
			for (Writer wr : freqWriterMap.values()) {
				//This is just in case the program crashed
				if (wr != null) {
					wr.flush();
					wr.close();
				}
			}
			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);
			
			System.out.println(PerfMon.asString());
		}
	}

}
