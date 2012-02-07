package uwaterloo.mdc.stats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;

public class CalcPerUserStats {

	private static final String EMPTY_COUNTS_LINE = "\t?\t?\t?";

	private String dataRoot = "P:\\mdc-datasets\\stats_counts";
	private String outPath = "P:\\mdc-datasets\\stats_summary";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO dataRoot from args

		CalcPerUserStats app = new CalcPerUserStats();
		Arrays.sort(args);
		if (Arrays.binarySearch(args, "--count") >= 0) {
			app.count();
		} else if (Arrays.binarySearch(args, "--summary") >= 0) {
			app.summaryStats();
		} else {
			System.out.println("Ussage: --count OR --summary");
		}

	}

	Map<String, Boolean> freqWriterLocks = Collections
			.synchronizedMap(new HashMap<String, Boolean>());

	Map<String, Writer> freqWriterMap = Collections
			.synchronizedMap(new HashMap<String, Writer>());

	private void count() throws Exception {
		try {
			// To make sure the class is loaded
			System.out.println(PerfMon.asString());

			CallableOperationFactory<SummaryStatistics, Frequency> factory = new CallableOperationFactory<SummaryStatistics, Frequency>();

			ExecutorService exec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);

			CompletionService<SummaryStatistics> ecs = new ExecutorCompletionService<SummaryStatistics>(
					exec);
			int numberTasks = 0;

			// FilenameFilter testFilter = new FilenameFilter() {
			// @Override
			// public boolean accept(File arg0, String arg1) {
			// return arg1.contains("media");
			// }
			// };

			File dataRootFile = FileUtils.getFile(dataRoot);
			for (File userDir : dataRootFile.listFiles()) {
				for (File dataFile : userDir.listFiles()) { // testFilter)) {
					// "accel.csv".equals(dataFile.getName()) ||
					if ("distance_matrix.csv".equals(dataFile.getName())) {
						continue;
					}

					PerUserDistinctValues distinctValues = (PerUserDistinctValues) factory
							.createOperation(PerUserDistinctValues.class, this,
									dataFile, outPath);

					ecs.submit(distinctValues);
					++numberTasks;
				}
			}

			Writer statWriter = Channels
					.newWriter(
							FileUtils
									.openOutputStream(
											FileUtils
													.getFile(outPath,
															"time-differnce-bet-samples_summary-stats.csv"))
									.getChannel(), Config.OUT_CHARSET);
			try {
				statWriter
						.append("id\tmin\tmax\tmean\tgeometric_mean\tn\tstandard_deviation\tvariance\tsecond_moment\n");
				for (int i = 0; i < numberTasks; ++i) {

					System.out.println(PerfMon.asString());

					Future<SummaryStatistics> finished = ecs.take();
					SummaryStatistics stat = finished.get();
					if (stat != null) {
						// There is no way to get the original callable or its
						// params
						// TODO: add it to the statistic throw a wrapper class
						statWriter.append("" + i).append('\t')
								.append("" + stat.getMin()).append('\t')
								.append("" + stat.getMax()).append('\t')
								.append("" + stat.getMean()).append('\t')
								.append("" + stat.getGeometricMean())
								.append('\t').append("" + stat.getN())
								.append('\t')
								.append("" + stat.getStandardDeviation())
								.append('\t').append("" + stat.getVariance())
								.append('\t')
								.append("" + stat.getSecondMoment())
								.append('\n');
					}
				}
			} finally {
				statWriter.flush();
				statWriter.close();
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
				// This is just in case the program crashed
				if (wr != null) {
					wr.flush();
					wr.close();
				}
			}
			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);

			System.out.println(PerfMon.asString());
			System.out.println("Done!");
		}
	}

	/**
	 * Actually there are no quantitative values at all, so : min, max, mean,
	 * geometric mean, n, sum, sum of squares, standard deviation, variance,
	 * percentiles, skewness, kurtosis, median are all irrelevant.. we do that
	 * the results from count
	 * 
	 * @throws Exception
	 */
	private void summaryStats() throws Exception {

		class UserIdFilter implements FilenameFilter {
			int id;

			UserIdFilter(int id) {
				this.id = id;
			}

			@Override
			public boolean accept(File dir, String name) {
				if (name.contains("wlan-ssid") || name.contains("accel-avdelt")
						|| name.contains("mac_") || name.contains("name")
						|| name.contains("gsm-cell_id")
						|| name.contains("last_mod")
						|| name.contains("process-path")
						|| name.contains("accel-data")
						|| name.contains("signaldbm")
						|| name.contains("calllog-duration")
						|| name.contains("calllog-number")
						|| name.contains("contacts")) {
					return false; // skip those huge files in the concatenation
				}
				return name.contains(numberToId(id));
			}

		}

		File dataRootFile = FileUtils.getFile(dataRoot);

		Writer resultWriter = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath,
								"counts_allinputs_allusers.csv")).getChannel(),
				Config.OUT_CHARSET);
		try {
			resultWriter.append("userid");

			int currMaxCols = 0;
			File[] headerFiles = null;
			for (int i = 0; i < 200; ++i) {
				FilenameFilter headerFilter = new UserIdFilter(i);
				File[] tempFiles = dataRootFile.listFiles(headerFilter);
				if (tempFiles.length > currMaxCols) {
					currMaxCols = tempFiles.length;
					headerFiles = tempFiles;
					System.out.println("Current max is: " + currMaxCols
							+ " for user: " + i);
				}
			}
			Arrays.sort(headerFiles);

			int numCols = headerFiles.length;

			for (int j = 0; j < numCols; ++j) {
				String pfx = headerFiles[j].getName();

				pfx = pfx.substring(0, pfx.lastIndexOf("-"));
				resultWriter.append('\t').append(pfx).append("-value")
						.append('\t').append(pfx).append("-count").append('\t')
						.append(pfx).append("-perctg");
			}
			resultWriter.append('\n');

			for (int i = 1; i < 200; ++i) {
				FilenameFilter userFilter = new UserIdFilter(i);
				File[] userFiles = dataRootFile.listFiles(userFilter);

				if (userFiles.length == 0) { // || userFiles.length != numCols){
					// System.out.println("User " + i +
					// " with less columnss.. bastard!");
					continue; // no files for this user (id > max)
				}

				Arrays.sort(userFiles);

				BufferedReader[] buffReader = new BufferedReader[userFiles.length];
				for (int j = 0; j < userFiles.length; ++j) {
					Reader userReader = Channels.newReader(FileUtils
							.openInputStream(FileUtils.getFile(userFiles[j]))
							.getChannel(), Config.OUT_CHARSET); // This is
																// output from
																// earlier
					buffReader[j] = new BufferedReader(userReader);
					buffReader[j].readLine(); // the header line
				}
				boolean allFilesConsumed = false;
				while (!allFilesConsumed) {
					allFilesConsumed = true;

					StringBuilder resultLine = new StringBuilder();
					resultLine.append(quote(numberToId(i)));

					for (int j = 0; j < userFiles.length; ++j) {
						int k = j;

						while (!userFiles[j]
								.getName()
								.substring(0,
										userFiles[j].getName().length() - 8)
								.equals(headerFiles[k].getName().substring(0,
										headerFiles[k].getName().length() - 8))) {
							resultLine.append(EMPTY_COUNTS_LINE);
							++k;
						}

						String featureLine = buffReader[j].readLine();
						if (featureLine != null) {
							String[] featureTokens = featureLine.split("\\\t");
							resultLine.append('\t')
									.append(quote(featureTokens[1]))
									// value
									.append('\t').append(featureTokens[2])
									.append('\t').append(featureTokens[3]);
							// resultLine.append(featureLine.substring(featureLine
							// .indexOf('\t')));
							allFilesConsumed = false;
							// System.out.println("Still has values:"+userFiles[j].getName());
						} else {
							resultLine.append(EMPTY_COUNTS_LINE);
						}

					}
					if (!allFilesConsumed) {
						resultWriter.append(resultLine.toString()).append('\n');
					}
				}
			}
		} finally {
			resultWriter.flush();
			resultWriter.close();
		}
	}

	private String quote(String orig) {
		return "\"" + orig + "\"";
	}

	public String numberToId(int number) {
		String userId;
		if (number < 10) {
			userId = "00" + number;
		} else if (number < 100) {
			userId = "0" + number;
		} else {
			userId = "" + number;
		}
		return userId;
	}
}
