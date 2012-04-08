package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.util.NotifyStream;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.EvaluationUtils;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

public class ParallelEvaluate implements Callable<Void> {
	
	private static String trainingPath= "C:\\mdc-datasets\\weka\\segmented_user";
	static Instances trainingSet;
	/**
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException  {
		ExecutorService appExec = Executors.newFixedThreadPool(Config.NUM_THREADS);
//				.newSingleThreadExecutor();
		Future<Void> lastFuture = null;
		
		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig,
				"ClassifyAndFeatSelect");
		try {
			System.setErr(new PrintStream(notifyStream));
			
			File trainingDir = FileUtils.getFile(trainingPath);
			ArrayList<File> trainingFiles = new ArrayList<File>();
			trainingFiles.addAll(Arrays.asList(trainingDir
					.listFiles(new FilenameFilter() {

						@Override
						public boolean accept(File dir, String name) {

							return name.endsWith(".arff");
						}
					})));

			trainingSet = loadAugmentedInstances(trainingFiles);
	
			
			
			
			lastFuture.get();
			appExec.shutdown();
			while (!appExec.isTerminated()) {
				// System.out.println("Shutting down");
				Thread.sleep(5000);
			}
			System.err.println("Finished running at " + new Date());
		} finally {
			try {
				notifyStream.close();
			} catch (IOException ignored) {

			}
			System.setErr(errOrig);
		}
	}

	static Instances loadAugmentedInstances(ArrayList<File> dataFiles)
			throws IOException {

		Instances trainingSet = null;
		int userIx = 0;
		for (File userData : dataFiles) {
			if (userIx == Config.NUM_USERS_TO_PROCESS) {
				break;
			}

			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(userData);

			// load structure
			Instances dataStruct = dataLoader.getStructure();
			dataStruct.setClassIndex(dataStruct.numAttributes() - 1);

			if (userIx == 0) {
				trainingSet = new Instances(dataStruct);
			}

			// load data
			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {

				trainingSet.add(dataInst);
			}

			++userIx;
		}
		return trainingSet;
	}

	
	final Class<? extends Classifier> baseClassifierClazz;
	
	public ParallelEvaluate(Class<? extends Classifier> baseClassifierClazz) {
		this.baseClassifierClazz = baseClassifierClazz;
	}
	
	@Override
	public Void call() throws Exception {
		// WE CAN'T use any of Weka's shit... coz they randomize all the time
		Evaluation eval = new Evaluation(trainingSet);
		return null;
	}
}
