package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;

class ImportIntoMallet {

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "P:\\mdc-datasets\\segmented_user-time";

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException, InterruptedException {
		ImportIntoMallet app = new ImportIntoMallet();
		app.createDocuments();
	}

	private void createDocuments() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException, InterruptedException {
		try {
			// To make sure the class is loaded
			System.out.println(PerfMon.asString());

			CallableOperationFactory<Void, Long> factory = new CallableOperationFactory<Void, Long>();

			ExecutorService exec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);

			CompletionService<Void> ecs = new ExecutorCompletionService<Void>(
					exec);
//			int numberTasks = 0;

			File dataRootFile = FileUtils.getFile(dataRoot);
			for (File userDir : dataRootFile.listFiles()) {
				File visitsFile = FileUtils.getFile(userDir,
						"visit_sequence_10min.csv");
				CreateDocumentsFromVisits fromVisits = (CreateDocumentsFromVisits) factory
						.createOperation(CreateDocumentsFromVisits.class, this,
								visitsFile, outPath);

				ecs.submit(fromVisits);
//				++numberTasks;
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
