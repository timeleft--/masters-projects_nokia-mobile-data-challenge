package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.operations.PrintStatsCallable;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.mallet.AuthorTopicAnalysis;

public class ImportIntoMallet {

	public static class MalletImportedFiles implements FileFilter {

		@Override
		public boolean accept(File file) {
			String fName = file.getName();
			boolean result = !("distance_matrix.csv".equals(fName))
					&& !("wlan.csv".equals(fName))
					&& !(fName.startsWith("visit_sequence_"))
					&& !("contacts.csv".equals(fName))
					&& !("gsm.csv".equals(fName))
					&& !("media.csv".equals(fName))
					&& !("process.csv".equals(fName));

			return result;
		}
	};

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";
	private String statsPath = "C:\\mdc-datasets\\mallet\\stats";

	private Map<String, Writer> statWriters = Collections
			.synchronizedMap(new HashMap<String, Writer>());

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

		ImportIntoMallet app = new ImportIntoMallet();
		app.createDocuments();
		
		CountConditionalFreqs countCond = new CountConditionalFreqs();
		ExecutorService countExec = Executors.newSingleThreadExecutor();
		countExec.submit(countCond);
		
		AuthorTopicAnalysis lda = new AuthorTopicAnalysis();
		ExecutorService ldaExec = Executors.newSingleThreadExecutor();
		ldaExec.submit(lda);
		
		ldaExec.shutdown();
		while(!ldaExec.isTerminated()){
			Thread.sleep(5000);
		}
		
		countExec.shutdown();
		while(!countExec.isTerminated()){
			Thread.sleep(5000);
		}
	}

	public ImportIntoMallet() throws IOException {
		File outDir = FileUtils.getFile(outPath);
		if(outDir != null){
		FileUtils.deleteDirectory(outDir);
	}}

	public void createDocuments() throws Exception {
		try {
			// To make sure the class is loaded
			System.out.println(PerfMon.asString());

			CallableOperationFactory<String, Long> fromVisitsFactory = new CallableOperationFactory<String, Long>();
			ExecutorService fromVisitsExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS / 4);
			CompletionService<String> fromVisitsEcs = new ExecutorCompletionService<String>(
					fromVisitsExec);

			CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, String> fromWlanFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, String>();
			ExecutorService fromWlanExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS / 4);
			CompletionService<KeyValuePair<String, HashMap<String, Object>>> fromWlanEcs = new ExecutorCompletionService<KeyValuePair<String, HashMap<String, Object>>>(
					fromWlanExec);

			CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> loadDataFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder>();
			ExecutorService loadDataExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS / 4);
			CompletionService<KeyValuePair<String, HashMap<String, Object>>> loadDataEcs = new ExecutorCompletionService<KeyValuePair<String, HashMap<String, Object>>>(
					loadDataExec);

			ExecutorService printStatsExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS / 4);
			CompletionService<Void> printStatsEcs = new ExecutorCompletionService<Void>(
					printStatsExec);

			int numLoadDataTasks = 0;
			int numberWifiTasks = 0;
			int fromVisitsNumberTasks = 0;
			int printTasks = 0;

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
					if (fromVisitsNumberTasks >= Config.NUM_USERS_TO_PROCESS) {
						break;
					}
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

					Future<KeyValuePair<String, HashMap<String, Object>>> finishedWlan = fromWlanEcs
							.take();
					KeyValuePair<String, HashMap<String, Object>> wlanStat = finishedWlan
							.get();
					if (wlanStat != null) {
						HashMap<String, Object> wlanStatMap = wlanStat
								.getValue();
						for (String wlanStatKey : wlanStatMap.keySet()) {

							Object wlanStatObj = wlanStatMap.get(wlanStatKey);

							PrintStatsCallable printStatCall = new PrintStatsCallable(
									wlanStatObj, wlanStat.getKey(),
									wlanStatKey, statWriters, statsPath);
							printStatsEcs.submit(printStatCall);
							++printTasks;
						}

						// /////////////////////////////////////////////////////
						// start filling the user documents with data
						String userid = wlanStat.getKey();
						File userDir = FileUtils.getFile(dataRootFile, userid);

						FileFilter dataFileFilter = new MalletImportedFiles();

						for (File dataFile : userDir.listFiles(dataFileFilter)) {
							LoadInputsIntoDocs loadDataOp = (LoadInputsIntoDocs) loadDataFactory
									.createOperation(LoadInputsIntoDocs.class,
											this, dataFile, outPath);
							loadDataEcs.submit(loadDataOp);
							++numLoadDataTasks;
						}
					}
				}

				// ////////// Print load stats
				for (int j = 0; j < numLoadDataTasks; ++j) {

					System.out.println(PerfMon.asString());

					Future<KeyValuePair<String, HashMap<String, Object>>> finishedLoading = loadDataEcs
							.take();
					KeyValuePair<String, HashMap<String, Object>> loadDataStat = finishedLoading
							.get();
					if (loadDataStat != null) {
						HashMap<String, Object> loadDataStatMap = loadDataStat
								.getValue();
						for (String loadDataStatKey : loadDataStatMap.keySet()) {

							Object loadDataStatObj = loadDataStatMap
									.get(loadDataStatKey);

							PrintStatsCallable printStatCall = new PrintStatsCallable(
									loadDataStatObj, loadDataStat.getKey(),
									loadDataStatKey, statWriters, statsPath);
							printStatsEcs.submit(printStatCall);
							++printTasks;
						}
					}
				}

				for (int i = 0; i < printTasks; ++i) {
					System.out.println(PerfMon.asString());
					printStatsEcs.take();
				}

				// This will make the executor accept no new threads
				// and finish all existing threads in the queue
				printStatsExec.shutdown();
				// Wait until all threads are finish
				while (!printStatsExec.isTerminated()) {
					Thread.sleep(5000);
					System.out.println(PerfMon.asString());
				}

				// This will make the executor accept no new threads
				// and finish all existing threads in the queue
				loadDataExec.shutdown();
				// Wait until all threads are finish
				while (!loadDataExec.isTerminated()) {
					Thread.sleep(5000);
					System.out.println(PerfMon.asString());
				}

				// This will make the executor accept no new threads
				// and finish all existing threads in the queue
				fromWlanExec.shutdown();
				// Wait until all threads are finish
				while (!fromWlanExec.isTerminated()) {
					Thread.sleep(5000);
					System.out.println(PerfMon.asString());
				}

				// This will make the executor accept no new threads
				// and finish all existing threads in the queue
				fromVisitsExec.shutdown();
				// Wait until all threads are finish
				while (!fromVisitsExec.isTerminated()) {
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

}
