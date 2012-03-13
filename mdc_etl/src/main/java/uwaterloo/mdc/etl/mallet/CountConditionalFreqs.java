package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.operations.PrintStatsCallable;
import uwaterloo.mdc.etl.util.KeyValuePair;
import cc.mallet.types.Instance;

//import uwaterloo.mdc.etl.mallet.*;

public class CountConditionalFreqs implements Callable<Void> {

	private String statsPath = "C:\\mdc-datasets\\mallet\\stats";
	private String shotColNamesPath = "C:\\mdc-datasets\\short-col-names.properties";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";

	private Map<String, Writer> statWriters = Collections
			.synchronizedMap(new HashMap<String, Writer>());
	private Properties shortColNameDict = new Properties();
	private ExecutorService printExec;
	// private CompletionService<Void> printEcs;
	private ExecutorService countExec;
	// private CompletionService<KeyValuePair<String, HashMap<Integer,
	// HashMap<String, Frequency>>>> countEcs;

	private Map<String, HashMap<String, Comparable<?>>> valueDomainMap;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));
		Config.quantizedFields = new Properties();
		Config.quantizedFields.load(FileUtils.openInputStream(FileUtils.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));
		
		CountConditionalFreqs app = new CountConditionalFreqs();

		app.call();
	}

	public Void call() throws Exception {

		this.count();

		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		this.printExec.shutdown();
		// Wait until all threads are finish
		while (!this.printExec.isTerminated()) {
			Thread.sleep(5000);
			System.out.println("Shutting down");
		}

		for (Writer wr : this.statWriters.values()) {
			if (wr != null) {
				wr.flush();
				wr.close();
			}
		}

		this.countExec.shutdown();
		return null;
	}

	public CountConditionalFreqs() throws IOException {
		shortColNameDict.load(FileUtils.openInputStream(FileUtils
				.getFile(shotColNamesPath)));

		countExec = Executors.newFixedThreadPool(Config.NUM_THREADS / 2);
		// countEcs = new ExecutorCompletionService<KeyValuePair<String,
		// HashMap<Integer, HashMap<String, Frequency>>>>(
		// countExec);
		printExec = Executors.newFixedThreadPool(Config.NUM_THREADS / 2);
		// printEcs = new ExecutorCompletionService<Void>(printExec);

		// init the enumMap
		CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> loadDataFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder>();
		for (Object fileColnameObj : shortColNameDict.values()) {
			String fileColname = ((String) fileColnameObj);
			String fileName = fileColname
					.substring(0, fileColname.indexOf('_'));
			loadDataFactory.loadClass(LoadInputsIntoDocs.class, fileName);
		}

		valueDomainMap = Collections
				.synchronizedMap(new HashMap<String, HashMap<String, Comparable<?>>>());
		for (String statKey : Discretize.enumsMap.keySet()) {
			HashMap<String, Comparable<?>> valueDomain = new HashMap<String, Comparable<?>>();
			valueDomainMap.put(statKey, valueDomain);

			for (Enum<?> enumVal : Discretize.enumsMap.get(statKey)) {
				valueDomain.put(enumVal.toString(), enumVal);
			}
		}

	}

	public void count() throws Exception {
		File dataDir = FileUtils.getFile(inputPath);
		int countingJobs = 0;
		int printingJobs = 0;

		ArrayList<Future<KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>>>> countedFutures = new ArrayList<Future<KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>>>>();
		for (File userDir : dataDir.listFiles()) {
			CountingCallable countCall = new CountingCallable(userDir);
			countedFutures.add(countExec.submit(countCall));
			// countEcs.submit(countCall);
			++countingJobs;
			// // Testing
			// if (countingJobs == 10)
			// break;
		}
		for (int c = 0; c < countingJobs; ++c) {
			Future<KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>>> targetStatsFuture = countedFutures
					.get(c);
			// Future<KeyValuePair<String, HashMap<Integer, HashMap<String,
			// Frequency>>>> targetStatsFuture = countEcs
			// .take();
			if (targetStatsFuture == null) {
				System.out.println("null future!!!");
				continue;
			}
			KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>> targetStatsPair = targetStatsFuture
					.get();
			if (targetStatsPair == null) {
				System.out.println("null Pair!!!");
				continue;
			}
			for (int i = 1; i <= 10; ++i) {
				HashMap<String, Frequency> freqMap = targetStatsPair.getValue()
						.get(i);
				for (String freqKey : freqMap.keySet()) {
					Frequency freq = freqMap.get(freqKey);
					PrintStatsCallable freqPrint = new PrintStatsCallable(freq,
							targetStatsPair.getKey(), freqKey, statWriters,
							statsPath + "\\cond_" + i);
					printExec.submit(freqPrint);
					// printEcs.submit(freqPrint);
					++printingJobs;
				}
			}
		}

		// many freq per result
//		if (printingJobs != countingJobs) {
//			throw new Exception("You printed  stats only : " + printingJobs
//					+ " time, you needed: " + countingJobs);
//		}

		// for (int i = 0; i < printingJobs; ++i) {
		// printEcs.take();
		// }
	}

	private class CountingCallable
			implements
			Callable<KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>>> {
		private final HashMap<Integer, HashMap<String, Frequency>> targetStats;
		private final File userDir;

		public KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>> call()
				throws Exception {
			// initStatMap();

			VisitsIterator userVisitsIter = new VisitsIterator(userDir);
			while (userVisitsIter.hasNext()) {
				Instance inst = userVisitsIter.next();
				char[] dataChars = ((String) inst.getData()).toCharArray();
				StringBuilder buffer = new StringBuilder();
				for (int i = 0; i < dataChars.length; ++i) {
					if (Character.isLowerCase(dataChars[i])
					// Make sure this is not an integer valued feature
							|| (Character.isDigit(dataChars[i])
									&& buffer.indexOf("hod") == -1
									&& buffer.indexOf("num") == -1 && buffer
									.indexOf("avg") == -1
							// we don't keep stats of standard dev: &&
							// buffer.indexOf("sdv") == -1
							)) {
						buffer.append(dataChars[i]);

					} else {
						String pfx = buffer.toString();
						String statKey = shortColNameDict.getProperty(pfx);
						buffer.setLength(0);

						for (; i < dataChars.length; ++i) {
							if (Character.isWhitespace(dataChars[i])) {
								break;
							}
							buffer.append(dataChars[i]);
						}

						String value = buffer.toString();
						buffer.setLength(0);

						// /

						HashMap<String, Comparable<?>> valueDomain = valueDomainMap
								.get(statKey);
						Comparable<?> enumVal;

						if (valueDomain != null) {
							enumVal = valueDomain.get(value);
						} else if (statKey != null
								&& statKey
										.endsWith(Config.RESULT_POSTFX_INTEGER)) {
							enumVal = Double.parseDouble(value);
						} else {
							continue; // apu and bma is ok to skip.. we don't
										// keep stats for it!
						}

						HashMap<String, Frequency> freqMap = targetStats
								.get(Integer.parseInt(inst.getTarget()
										.toString()));
						Frequency freq = freqMap.get(statKey);
						freq.addValue(enumVal);
					}
				}
			}
			return new KeyValuePair<String, HashMap<Integer, HashMap<String, Frequency>>>(
					userDir.getName(), targetStats);
		}

		protected CountingCallable(File userDir) {
			// targetStats.clear();
			this.userDir = userDir;

			targetStats = new HashMap<Integer, HashMap<String, Frequency>>();
			for (int i = 1; i <= 10; ++i) {
				HashMap<String, Frequency> freqMap = new HashMap<String, Frequency>();
				for (String statKey : Discretize.enumsMap.keySet()) {
					freqMap.put(statKey, new Frequency());
				}
				freqMap.put(Config.RESULT_KEY_NUM_MICRO_LOCS_FREQ,
						new Frequency());
				freqMap.put(Config.RESULT_KEY_AVG_APS_FREQ, new Frequency());
				freqMap.put(Config.RESULT_KEY_AVG_BTS_FREQ, new Frequency());
				targetStats.put(i, freqMap);
			}

		}
	}

}
