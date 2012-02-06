package uwaterloo.mdc.stats;

import java.io.File;
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

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "P:\\mdc-datasets\\stats";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO dataRoot from args

		CalcPerUserStats app = new CalcPerUserStats();
		Arrays.sort(args);
		// if(Arrays.binarySearch(args, "--count") >= 0){
		app.count();
		// } else
		// if(Arrays.binarySearch(args, "--summary") >= 0){
		// app.summaryStats();
		// } else {
		// System.out.println("Ussage: --count OR --summary");
		// }

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

			Writer statWriter = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, "time-differnce-bet-samples_summary-stats.csv"))
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
		}
	}

	// Actually there are no quantitative values at all, so :
	// min, max, mean, geometric mean, n, sum, sum of squares, standard
	// deviation, variance, percentiles, skewness, kurtosis, median
	// are all irrelevant.. maybe do that the results from count
	// private void summaryStats() throws Exception {
	//
	// }

}
