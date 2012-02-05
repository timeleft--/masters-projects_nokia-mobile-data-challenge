package uwaterloo.mdc.stats;

import java.io.File;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;

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

	Map<String, Writer> freqWriterMap = Collections
			.synchronizedMap(new HashMap<String, Writer>());

	private void count() throws Exception {
		try {
			// TODO: ends with time or equals start is the time
			ExecutorService exec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);

			File dataRootFile = FileUtils.getFile(dataRoot);
			for (File userDir : dataRootFile.listFiles()) {
				for (File dataFile : userDir.listFiles()) {
					if ("distance_matrix.csv".equals(dataFile.getName())) {
						continue;
					}

					ExtractDistinctValues distinctValues = new ExtractDistinctValues(
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
			}
		} finally {
			for (Writer wr : freqWriterMap.values()) {
				// The use of anything itself as the lock to using it
				// is definitely not the best thing to do, and is prone
				// to null pointer exceptions.. TODO: use lock map
				if (wr != null) {
					wr.flush();
					wr.close();
				}
			}
		}
	}

}
