package uwaterloo.mdc.weka;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;

public class ClassifyAndFeatSelect implements Callable<Void> {

	private class FoldCallable implements Callable<Double[]> {

		final Classifier baseClassifier;
		
		final int v;

		public FoldCallable(int fold) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
			this.v = fold;
			baseClassifier = baseClassifierClazz.getConstructor().newInstance();
		}

		@Override
		public Double[] call() throws Exception {
			Double[] accuracy = { 0.0, 0.0 };

			Frequency[] foldConfusionMatrix;
			foldConfusionMatrix = new Frequency[Config.NUM_LABELS_CONSIDERED];
			for (int i = 0; i < foldConfusionMatrix.length; ++i) {
				foldConfusionMatrix[i] = new Frequency();
			}

			Frequency[] foldFeactSelectCM;
			foldFeactSelectCM = new Frequency[Config.NUM_LABELS_CONSIDERED];
			for (int i = 0; i < foldFeactSelectCM.length; ++i) {
				foldFeactSelectCM[i] = new Frequency();
			}

			int foldStart = v * Config.VALIDATION_FOLD_WIDTH;

			Collection<File> inputArrfs = FileUtils.listFiles(
					FileUtils.getFile(inPath), new String[] { "arff" }, true);

			Instances validationSet = null;
			Instances trainingSet = null;

			// train Classifier
			boolean firstUser = true;
			int userIx = 0;
			for (File userData : inputArrfs) {

				if (userIx == (foldStart + inFoldTestIx)) {
					validationSet = new Instances(Channels.newReader(FileUtils
							.openInputStream(userData).getChannel(),
							Config.OUT_CHARSET));
					validationSet
							.setClassIndex(validationSet.numAttributes() - 1);
				} else {

					ArffLoader dataLoader = new ArffLoader();
					dataLoader.setFile(userData);

					// load structure
					Instances structure = dataLoader.getStructure();
					structure.setClassIndex(structure.numAttributes() - 1);

					if (firstUser) {
						if (baseClassifier instanceof UpdateableClassifier) {
							baseClassifier.buildClassifier(structure);
						} else {
							trainingSet = new Instances(structure);
						}
					}

					// load data
					Instance current;
					while ((current = dataLoader.getNextInstance(structure)) != null) {
						if(current.classIsMissing()){
							continue;
						}
					
						if (baseClassifier instanceof UpdateableClassifier) {
							((UpdateableClassifier) baseClassifier)
									.updateClassifier(current);
						} else {
							trainingSet.add(current);
						}

						// Not supported :(
						// baseClassifier.updateClassifier(dataLoader.getDataSet());

					}
				}
				System.out.println((System.currentTimeMillis() - startTime)
						+ " (fold " + v + "): Done reading user: "
						+ userData.getName());
				++userIx;
			}

			if (baseClassifier instanceof UpdateableClassifier) {
				// already trained
			} else {
				baseClassifier.buildClassifier(trainingSet);
				trainingSet = null;
			}

			System.out.println((System.currentTimeMillis() - startTime)
					+ " (fold " + v + "): Finished training for fold: " + v);
			Writer classificationsWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_classifications.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {
				classificationsWr
						.append("instance\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");
				if (validationSet.numInstances() == 0) {
					classificationsWr.append("No validation data for fold: "
							+ v);

				} else {
					int trueClassificationsCount = 0;

					for (int i = 0; i < validationSet.numInstances(); ++i) {
						Instance vInst = validationSet.instance(i);

						if (vInst.classIsMissing()) {
							continue;
						}

						Instance classMissing = (Instance) vInst.copy();
						classMissing.setDataset(vInst.dataset());
						classMissing.setClassMissing();

						double[] vClassDist = baseClassifier
								.distributionForInstance(classMissing);
						classificationsWr.append(vInst.dataset().relationName()
								+ "[" + i + "]");
						double vClassMaxProb = Double.NEGATIVE_INFINITY;
						double vClass = -1;
						for (int j = 0; j < vClassDist.length; ++j) {
							classificationsWr.append("\t" + vClassDist[j]);
							if (vClassDist[j] > vClassMaxProb) {
								vClassMaxProb = vClassDist[j];
								vClass = j + 1;
							}
						}
						classificationsWr.append('\n');
						// The class "Value" is actually its index!!!!!!
						if (vClass == vInst.classValue() + 1) {
							++trueClassificationsCount;
						}
						long trueLabelCfMIx = Math.round(vInst.classValue());
						long bestLabelInt = Math.round(vClass);
						foldFeactSelectCM[(int) trueLabelCfMIx]
								.addValue(bestLabelInt);
						synchronized (totalConfusionMatrix) {
							totalConfusionMatrix[(int) trueLabelCfMIx]
									.addValue(bestLabelInt);
						}
					}
					accuracy[0] = trueClassificationsCount * 1.0
							/ validationSet.numInstances();
					synchronized (cvClassificationAccuracyWr) {

						cvClassificationAccuracyWr
								.append(Integer.toString(v))
								.append('\t')
								.append(Integer
										.toString(trueClassificationsCount))
								.append('\t')
								.append(Integer.toString(validationSet
										.numInstances())).append('\t')
								.append(Double.toString(accuracy[0]))
								.append('\n');
					}

				}
			} finally {
				classificationsWr.flush();
				classificationsWr.close();
			}
			Writer foldConfusionWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_confusion-matrix.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {
				writeConfusionMatrix(foldConfusionWr, foldFeactSelectCM);
			} finally {
				foldConfusionWr.flush();
				foldConfusionWr.close();
			}
			System.out.println((System.currentTimeMillis() - startTime)
					+ " (fold " + v + "): Finished validation for fold: " + v);

			// //////////////////////////////////////

			AttributeSelectedClassifier featSelector = new AttributeSelectedClassifier();
			CfsSubsetEval eval = new CfsSubsetEval();
			GreedyStepwise search = new GreedyStepwise();
			search.setSearchBackwards(true);

			featSelector.setClassifier(baseClassifier);
			featSelector.setEvaluator(eval);
			featSelector.setSearch(search);

			Writer featSelectWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_feat-selected-classifications.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				featSelector.buildClassifier(validationSet);

				featSelectWr
						.append("instance\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");
				if (validationSet.numInstances() == 0) {
					featSelectWr.append("Not validation data for fold: " + v);

				} else {
					int featSelectCorrectCount = 0;

					for (int i = 0; i < validationSet.numInstances(); ++i) {
						Instance vInst = validationSet.instance(i);

						if (vInst.classIsMissing()) {
							continue;
						}

						Instance classMissing = (Instance) vInst.copy();
						classMissing.setDataset(vInst.dataset());
						classMissing.setClassMissing();

						double[] vClassDist = featSelector
								.distributionForInstance(classMissing);
						featSelectWr.append(vInst.dataset().relationName()
								+ "[" + i + "]");
						double vClassMaxProb = Double.NEGATIVE_INFINITY;
						double vClass = -1;
						for (int j = 0; j < vClassDist.length; ++j) {
							featSelectWr.append("\t" + vClassDist[j]);
							if (vClassDist[j] > vClassMaxProb) {
								vClassMaxProb = vClassDist[j];
								vClass = j + 1;
							}
						}
						featSelectWr.append('\n');
						// The class "Value" is actually its index!!!!!!
						if (vClass == vInst.classValue() + 1) {
							++featSelectCorrectCount;
						}
						long trueLabelCfMIx = Math.round(vInst.classValue());
						long bestLabelInt = Math.round(vClass);
						foldFeactSelectCM[(int) trueLabelCfMIx]
								.addValue(bestLabelInt);
						synchronized (totalFeatSelectCM) {
							totalFeatSelectCM[(int) trueLabelCfMIx]
									.addValue(bestLabelInt);
						}
					}
					accuracy[1] = featSelectCorrectCount * 1.0
							/ validationSet.numInstances();
					synchronized (cvFeatSelectAccuracyWr) {

						cvFeatSelectAccuracyWr
								.append(Integer.toString(v))
								.append('\t')
								.append(Integer
										.toString(featSelectCorrectCount))
								.append('\t')
								.append(Integer.toString(validationSet
										.numInstances())).append('\t')
								.append(Double.toString(accuracy[1]))
								.append('\n');
					}

				}
			} catch (WekaException e) {
				if (e.getMessage().startsWith("Not enough training instances")) {
					featSelectWr.append(e.getMessage());
				}
			} finally {
				featSelectWr.flush();
				featSelectWr.close();
			}

			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifier.getClass().getName(), "v" + v
							+ "_feat-selection.txt"), featSelector.toString());

			System.out.println((System.currentTimeMillis() - startTime)
					+ " (fold " + v
					+ "): Finished feature selection for fold: " + v);
			// //////////////////////////////////////

			return accuracy;
		}

	}

	private String outputPath = "C:\\mdc-datasets\\weka\\validation";
	private String inPath = "C:\\mdc-datasets\\weka\\segmented_user";

	private final Frequency[] totalConfusionMatrix;
	private final Frequency[] totalFeatSelectCM;
	private long startTime = System.currentTimeMillis();

	private final int inFoldTestIx = new Random(System.currentTimeMillis())
			.nextInt(Config.VALIDATION_FOLD_WIDTH);

	private final Writer cvClassificationAccuracyWr;
	private final Writer cvFeatSelectAccuracyWr;

	private Class<? extends Classifier> baseClassifierClazz;

	public ClassifyAndFeatSelect(Class<? extends Classifier> baseClassifierClazz) throws IOException {
		this.baseClassifierClazz = baseClassifierClazz;
		
		totalConfusionMatrix = new Frequency[Config.NUM_LABELS_CONSIDERED];
		for (int i = 0; i < totalConfusionMatrix.length; ++i) {
			totalConfusionMatrix[i] = new Frequency();
		}
		totalFeatSelectCM = new Frequency[Config.NUM_LABELS_CONSIDERED];
		for (int i = 0; i < totalFeatSelectCM.length; ++i) {
			totalFeatSelectCM[i] = new Frequency();
		}
		cvClassificationAccuracyWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath, baseClassifierClazz
								.getName(), "classification-accuracy-cv.txt"))
						.getChannel(), Config.OUT_CHARSET);
		cvFeatSelectAccuracyWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath, baseClassifierClazz
								.getName(), "feat-selected-accuracy-cv.txt"))
						.getChannel(), Config.OUT_CHARSET);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

		ClassifyAndFeatSelect app = new ClassifyAndFeatSelect(NaiveBayesUpdateable.class);
		app.call();
	}

	public Void call() throws Exception {
		this.acrossUsersClassify();

		return null;
	}

	public void acrossUsersClassify() throws Exception {

		SummaryStatistics accuracySummaryAllFeatures = new SummaryStatistics();
		SummaryStatistics accuracySummaryFeatSelected = new SummaryStatistics();

		cvClassificationAccuracyWr.append("vFold").append('\t').append("trueN")
				.append('\t').append("totalN").append('\t').append("accuracy")
				.append('\n');
		cvFeatSelectAccuracyWr.append("vFold").append('\t').append("trueN")
				.append('\t').append("totalN").append('\t').append("accuracy")
				.append('\n');
		try {

			System.out.println(new Date().toString()
					+ " (total): Validating with every user number "
					+ inFoldTestIx + " within every "
					+ Config.VALIDATION_FOLD_WIDTH + " users");
			ExecutorService foldExecutors = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			ArrayList<Future<Double[]>> foldFutures = new ArrayList<Future<Double[]>>();
			int numClassifyTasks = 0;
			for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
				FoldCallable FoldCallable = new FoldCallable(v);
				foldFutures.add(foldExecutors.submit(FoldCallable));
				++numClassifyTasks;
			}
			for (int i = 0; i < numClassifyTasks; ++i) {
				Double[] accuracies = foldFutures.get(i).get();
				accuracySummaryAllFeatures.addValue(accuracies[0]);
				accuracySummaryFeatSelected.addValue(accuracies[1]);
			}
			foldExecutors.shutdown();
			while (!foldExecutors.isTerminated()) {
				System.out.println((System.currentTimeMillis() - startTime)
						+ " (total): shutting down");
				Thread.sleep(1000);
			}
		} finally {
			cvClassificationAccuracyWr.flush();
			cvClassificationAccuracyWr.close();
			cvFeatSelectAccuracyWr.flush();
			cvFeatSelectAccuracyWr.close();
		}
		FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
				baseClassifierClazz.getName(), "accuracy-summary.txt"),
				accuracySummaryAllFeatures.toString());
		FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
				baseClassifierClazz.getName(),
				"feat-selected_accuracy-summary.txt"),
				accuracySummaryFeatSelected.toString());

		Writer totalConfusionWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath, baseClassifierClazz
								.getName(), "confusion-matrix.txt"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(totalConfusionWr, totalConfusionMatrix);
		} finally {
			totalConfusionWr.flush();
			totalConfusionWr.close();
		}

		Writer featSelectedConfusionWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath, baseClassifierClazz
								.getName(),
								"feat-selected-confusion-matrix.txt"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(featSelectedConfusionWr, totalFeatSelectCM);
		} finally {
			featSelectedConfusionWr.flush();
			featSelectedConfusionWr.close();
		}

		System.out.println(new Date().toString() + " (total): Done in "
				+ (System.currentTimeMillis() - startTime) + " millis");
	}

	public static void writeConfusionMatrix(Writer foldConfusionWr,
			Frequency[] foldConfusionMatrix) throws IOException {
		foldConfusionWr.append("label\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ttotal\n");
		for (int i = 0; i < foldConfusionMatrix.length; ++i) {
			foldConfusionWr.append(Integer.toString(i + 1));
			long totalCount = 0;
			for (int j = 1; j <= Config.NUM_LABELS_CONSIDERED; ++j) {
				long cnt = foldConfusionMatrix[i].getCount(j);
				totalCount += cnt;
				foldConfusionWr.append('\t').append(Long.toString(cnt));
			}
			foldConfusionWr.append('\t').append(Long.toString(totalCount))
					.append('\n');
		}
	}

}
