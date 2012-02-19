package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

class ImportIntoMallet {

	private static final String COUNT_PSTFX = "_count";
	private static final String PCTG_PSTFX = "_pctg";
	private static final String PERUSER_FREQ_PREFX = "per-user-freq_";
	private static final String PERUSER_SUMMART_PREFX = "per-user-summary_";

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "C:\\Users\\yaboulna\\mdc2\\segmented_user-time";
	private String statsPath = "C:\\Users\\yaboulna\\mdc2\\stats";

	private HashMap<String, Writer> statWriters = new HashMap<String, Writer>();

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, IOException, InterruptedException,
			ExecutionException {
		ImportIntoMallet app = new ImportIntoMallet();
		app.createDocuments();
	}

	private void createDocuments() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, IOException, InterruptedException,
			ExecutionException {
		try {
			// To make sure the class is loaded
			System.out.println(PerfMon.asString());

			CallableOperationFactory<String, Long> fromVisitsFactory = new CallableOperationFactory<String, Long>();
			ExecutorService fromVisitsExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			CompletionService<String> fromVisitsEcs = new ExecutorCompletionService<String>(
					fromVisitsExec);

			CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, String> fromWlanFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, String>();
			ExecutorService fromWlanExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			CompletionService<KeyValuePair<String, HashMap<String, Object>>> fromWlanEcs = new ExecutorCompletionService<KeyValuePair<String, HashMap<String, Object>>>(
					fromWlanExec);

			int numberWifiTasks = 0;
			int fromVisitsNumberTasks = 0;

			File fromWifiLogFil = FileUtils.getFile(Config.LOG_PATH,
					"RefineDocumentsFromWlan.log");
			Writer FromWifiLogWriter = Channels.newWriter(FileUtils
					.openOutputStream(fromWifiLogFil, true).getChannel(),
					Config.OUT_CHARSET);
			RefineDocumentsFromWlan.setLOG(FromWifiLogWriter);
			try {
				File dataRootFile = FileUtils.getFile(dataRoot);
				for (File userDir : dataRootFile.listFiles()) {
					File visitsFile = FileUtils.getFile(userDir,
							"visit_sequence_10min.csv");
					CreateDocumentsFromVisits fromVisits = (CreateDocumentsFromVisits) fromVisitsFactory
							.createOperation(CreateDocumentsFromVisits.class,
									this, visitsFile, outPath);

					fromVisitsEcs.submit(fromVisits);
					++fromVisitsNumberTasks;

				}

				for (int i = 0; i < fromVisitsNumberTasks; ++i) {
					System.out.println(PerfMon.asString());

					Future<String> finished = fromVisitsEcs.take();
					String userid = finished.get();
					if (userid != null) {
						File userDir = FileUtils.getFile(dataRootFile, userid);

						File wlanFile = FileUtils.getFile(userDir, "wlan.csv");
						RefineDocumentsFromWlan fromWlan = (RefineDocumentsFromWlan) fromWlanFactory
								.createOperation(RefineDocumentsFromWlan.class,
										this, wlanFile, outPath);

						fromWlanEcs.submit(fromWlan);
						++numberWifiTasks;
					}
				}

				for (int i = 0; i < numberWifiTasks; ++i) {

					System.out.println(PerfMon.asString());

					Future<KeyValuePair<String, HashMap<String, Object>>> finished = fromWlanEcs
							.take();
					KeyValuePair<String, HashMap<String, Object>> annotStat = finished
							.get();
					if (annotStat != null) {
						HashMap<String, Object> statMap = annotStat.getValue();
						for (String statKey : statMap.keySet()) {

							Object statObj = statMap.get(statKey);

							if (statObj instanceof Frequency) {
								writeStats(annotStat.getKey(), statKey,
										((Frequency) statObj));

							} else if (statObj instanceof SummaryStatistics) {
								writeStats(annotStat.getKey(), statKey,
										(SummaryStatistics) statObj);

							}
						}
					}
				}

				// This will make the executor accept no new threads
				// and finish all existing threads in the queue
				fromWlanExec.shutdown();
				// Wait until all threads are finish
				while (!fromWlanExec.isTerminated()) {
					Thread.sleep(5000);
					System.out.println(PerfMon.asString());
				}
			} finally {
				FromWifiLogWriter.flush();
				FromWifiLogWriter.close();
			}
		} finally {
			long delta = System.currentTimeMillis();
			for (Writer wr : statWriters.values()) {
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

	private void writeStats(String userid, String statKey,
			SummaryStatistics stat) throws IOException {
		long delta = System.currentTimeMillis();
		Writer summaryWriter = statWriters.get(statKey);
		if (summaryWriter == null) {
			summaryWriter = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(statsPath, PERUSER_SUMMART_PREFX
									+ statKey + ".csv")).getChannel(),
					Config.OUT_CHARSET);

			statWriters.put(statKey, summaryWriter);

			// write header
			summaryWriter
					.append(Config.USERID_COLNAME)
					.append("\tn\tmin\tmax\tmean\tstandard_deviation\tvariance\tgeometric_mean\tsecond_moment\n");
		}

		summaryWriter.append(userid).append('\t').append("" + stat.getN())
				.append('\t').append("" + stat.getMin()).append('\t')
				.append("" + stat.getMax()).append('\t')
				.append("" + stat.getMean()).append('\t')
				.append("" + stat.getStandardDeviation()).append('\t')
				.append("" + stat.getVariance()).append('\t')
				.append("" + stat.getGeometricMean()).append('\t')
				.append("" + stat.getSecondMoment()).append('\n');

		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.IO_WRITE, delta);
	}

	public synchronized void writeStats(String userid, String statKey,
			Frequency stat) throws IOException {
		long delta = System.currentTimeMillis();

		Enum<?>[] valsArr = Discretize.enumsMap.get(statKey);

		Writer freqWriter = statWriters.get(statKey);
		if (freqWriter == null) {
			freqWriter = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(statsPath, PERUSER_FREQ_PREFX
									+ statKey + ".csv")).getChannel(),
					Config.OUT_CHARSET);

			statWriters.put(statKey, freqWriter);

			// Write the header
			freqWriter.append(Config.USERID_COLNAME);

			for (int i = 0; i < valsArr.length; ++i) {
				Enum<?> val = valsArr[i];
				String valLabel = val.toString();
				freqWriter.append('\t')
						.append(StringUtils.quote(valLabel + COUNT_PSTFX))
						.append('\t')
						.append(StringUtils.quote(valLabel + PCTG_PSTFX));
			}

			freqWriter
					.append('\t')
					.append(StringUtils.quote(Config.MISSING_VALUE_PLACEHOLDER
							+ COUNT_PSTFX))
					.append('\t')
					.append(StringUtils.quote(Config.MISSING_VALUE_PLACEHOLDER
							+ PCTG_PSTFX));

			freqWriter.append('\n');
		}

		freqWriter.append(userid);

		for (int i = 0; i < valsArr.length; ++i) {
			Enum<?> val = valsArr[i];

			long valCnt = stat.getCount(val);
			double valPct = stat.getPct(val);

			freqWriter.append('\t').append(Long.toString(valCnt)).append('\t')
					.append(Double.toString(valPct));

		}

		freqWriter
				.append('\t')
				.append(Long.toString(stat
						.getCount(Config.MISSING_VALUE_PLACEHOLDER)))
				.append('\t')
				.append(Double.toString(stat
						.getPct(Config.MISSING_VALUE_PLACEHOLDER)));

		freqWriter.append('\n');

		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.IO_WRITE, delta);
	}
}
