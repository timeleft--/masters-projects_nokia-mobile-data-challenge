package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;

class ImportIntoMallet {

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "C:\\Users\\yaboulna\\mdc\\segmented_user-time";
	private String statsPath = "P:\\mdc-datasets\\stats";

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

			CallableOperationFactory<KeyValuePair<String, Frequency>, String> fromWlanFactory = new CallableOperationFactory<KeyValuePair<String, Frequency>, String>();
			ExecutorService fromWlanExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			CompletionService<KeyValuePair<String, Frequency>> fromWlanEcs = new ExecutorCompletionService<KeyValuePair<String, Frequency>>(
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

				Writer statWriter = Channels
						.newWriter(
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(statsPath,
																"per-user-freq_wlan_visit-novisit.csv"))
										.getChannel(), Config.OUT_CHARSET);
				try {
					statWriter
							.append("userid\tvisit_count\tvisit_pctg\tnovisit_count\tnovisit_pctg\n");
					for (int i = 0; i < numberWifiTasks; ++i) {

						System.out.println(PerfMon.asString());

						Future<KeyValuePair<String, Frequency>> finished = fromWlanEcs
								.take();
						KeyValuePair<String, Frequency> annotStat = finished
								.get();
						if (annotStat != null) {
							Frequency stat = annotStat.getValue();
							statWriter
									.append(annotStat.getKey())
									.append('\t')
									.append(""
											+ stat.getCount(RefineDocumentsFromWlan.FREQ_VISIT_WLAN_VAR))
									.append('\t')
									.append(""
											+ stat.getPct(RefineDocumentsFromWlan.FREQ_VISIT_WLAN_VAR))
									.append('\t')
									.append(""
											+ stat.getCount(RefineDocumentsFromWlan.FREQ_NOVISIT_WLAN_VAR))
									.append('\t')
									.append(""
											+ stat.getPct(RefineDocumentsFromWlan.FREQ_NOVISIT_WLAN_VAR))
									.append('\n');
						}
					}
				} finally {
					statWriter.flush();
					statWriter.close();
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
			// long delta = System.currentTimeMillis();
			// for (Writer wr : freqWriterMap.values()) {
			// // This is just in case the program crashed
			// if (wr != null) {
			// wr.flush();
			// wr.close();
			// }
			// }
			// delta = System.currentTimeMillis() - delta;
			// PerfMon.increment(TimeMetrics.IO_WRITE, delta);

			System.out.println(PerfMon.asString());
			System.out.println("Done!");
		}
	}
}
