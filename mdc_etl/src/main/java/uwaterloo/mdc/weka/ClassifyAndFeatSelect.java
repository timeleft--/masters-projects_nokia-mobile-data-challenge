package uwaterloo.mdc.weka;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.ConsistencySubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.attributeSelection.InfoGainAttributeEval;
import weka.attributeSelection.Ranker;
import weka.attributeSelection.ReliefFAttributeEval;
import weka.attributeSelection.SubsetEvaluator;
import weka.attributeSelection.WrapperSubsetEval;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.BayesianLogisticRegression;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.Logistic;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.meta.ClassificationViaClustering;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;

public class ClassifyAndFeatSelect implements Callable<Void> {
	// Class<? extends ASEvaluation>[] cannot create!
	@SuppressWarnings("rawtypes")
	public static Class[] attrSelectEvaluationClazzes = { CfsSubsetEval.class,
			// ChiSquaredAttributeEval.class,
			ConsistencySubsetEval.class,
			// FilteredAttributeEval.class, FilteredSubsetEval.class,
			// GainRatioAttributeEval.class,
			InfoGainAttributeEval.class, ReliefFAttributeEval.class,
			// SVMAttributeEval.class, SymmetricalUncertAttributeEval.class,
			WrapperSubsetEval.class, };

	// TODO Attribute Transformers: PrincipalComponents and
	// LatentSemanticAnalysis
	public static final String ALL_FEATS = "all-features";

	private class FoldCallable implements Callable<HashMap<String, Double>> {

		final Classifier baseClassifier;

		final int v;

		public FoldCallable(int fold) throws InstantiationException,
				IllegalAccessException, IllegalArgumentException,
				InvocationTargetException, NoSuchMethodException,
				SecurityException {
			this.v = fold;
			baseClassifier = baseClassifierClazz.getConstructor().newInstance();
		}

		@Override
		public HashMap<String, Double> call() throws Exception {
			HashMap<String, Double> accuracyMap = new HashMap<String, Double>();

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
				if (userIx == Config.NUM_USERS_TO_PROCESS) {
					break;
				}
				if (userData.getName().startsWith("113")) {
					continue; // too mcuh data, and might make us run out of
								// memory
				}

				File appData = FileUtils.getFile(FilenameUtils
						.removeExtension(userData.getAbsolutePath()) + ".app");

				if (userIx == (foldStart + inFoldTestIx)) {
					validationSet = new Instances(Channels.newReader(FileUtils
							.openInputStream(userData).getChannel(),
							Config.OUT_CHARSET));
					Reader appReader = Channels.newReader(FileUtils
							.openInputStream(appData).getChannel(),
							Config.OUT_CHARSET);
					validationSet = Instances.mergeInstances(new Instances(
							appReader), validationSet);
					validationSet
							.setClassIndex(validationSet.numAttributes() - 1);
					validationSet.setRelationName(FilenameUtils
							.removeExtension(userData.getName()));
				} else {
					ArffLoader dataLoader = new ArffLoader();
					dataLoader.setFile(userData);

					ArffLoader appLoader = new ArffLoader();
					appLoader.setFile(appData);

					// load structure
					Instances dataStruct = dataLoader.getStructure();
					Instances appStruct = appLoader.getStructure();
					Instances joinedStruct = Instances.mergeInstances(
							appStruct, dataStruct);
					joinedStruct
							.setClassIndex(joinedStruct.numAttributes() - 1);
					joinedStruct.setRelationName(FilenameUtils
							.removeExtension(userData.getName()));

					if (firstUser) {
						if (baseClassifier instanceof UpdateableClassifier) {
							baseClassifier.buildClassifier(joinedStruct);
						} else {
							trainingSet = new Instances(joinedStruct);
						}
					}

					// load data
					Instance dataInst;
					Instance appInst;
					int instIx = 0;
					while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {
						appInst = appLoader.getNextInstance(appStruct);

						if (appInst == null) {
							throw new Exception(
									"App Insances fewer than data instances: "
											+ instIx);
						}
						// isClassMissing but without haveing to set the clas
						if (dataInst.isMissing(dataInst.numAttributes() - 1)) {
							continue;
						}

						Instance joinedInst = dataInst.mergeInstance(appInst);
						joinedInst.setDataset(joinedStruct);

						if (baseClassifier instanceof UpdateableClassifier) {
							((UpdateableClassifier) baseClassifier)
									.updateClassifier(joinedInst);
						} else {
							trainingSet.add(joinedInst);
						}
						++instIx;
					}
					if (appLoader.getNextInstance(appStruct) != null) {
						throw new Exception(
								"App Insances more than data instances: "
										+ instIx);
					}
				}

				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Done reading user: " + userData.getName());
				++userIx;
			}

			if (baseClassifier instanceof UpdateableClassifier) {
				// already trained
			} else {
				baseClassifier.buildClassifier(trainingSet);
				trainingSet = null;
			}

			System.out.println(baseClassifierClazz.getSimpleName() + " - "
					+ (System.currentTimeMillis() - startTime) + " (fold " + v
					+ "): Finished training for fold: " + v);
			Writer classificationsWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_classifications.txt")).getChannel(),
					Config.OUT_CHARSET);

			try {
				classificationsWr
						.append("instance\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");

				// TODO: user 113
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
					accuracyMap.put(ALL_FEATS, trueClassificationsCount * 1.0
							/ validationSet.numInstances());
					synchronized (cvClassificationAccuracyWr) {

						cvClassificationAccuracyWr
								.append(Integer.toString(v))
								.append('\t')
								.append(Integer
										.toString(trueClassificationsCount))
								.append('\t')
								.append(Integer.toString(validationSet
										.numInstances()))
								.append('\t')
								.append(Double.toString(accuracyMap
										.get(ALL_FEATS))).append('\n');
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
			System.out.println(baseClassifierClazz.getSimpleName() + " - "
					+ (System.currentTimeMillis() - startTime) + " (fold " + v
					+ "): Finished validation for fold: " + v);

			// //////////////////////////////////////

			for (@SuppressWarnings("rawtypes")
			Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {

				AttributeSelectedClassifier featSelector = new AttributeSelectedClassifier();
				@SuppressWarnings("unchecked")
				ASEvaluation eval = (ASEvaluation) attrSelectEvalClazz
						.getConstructor().newInstance();
				ASSearch search;
				if (eval instanceof SubsetEvaluator) {
					search = new GreedyStepwise();
					((GreedyStepwise) search).setSearchBackwards(true);
				} else {
					search = new Ranker();
				}

				featSelector.setClassifier(baseClassifier);
				featSelector.setEvaluator(eval);
				featSelector.setSearch(search);

				Writer featSelectWr = Channels
						.newWriter(
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																outputPath,
																baseClassifier
																		.getClass()
																		.getName(),
																attrSelectEvalClazz
																		.getName(),
																"v"
																		+ v
																		+ "_feat-selected-classifications.txt"))
										.getChannel(), Config.OUT_CHARSET);
				try {
					featSelector.buildClassifier(validationSet);

					featSelectWr
							.append("instance\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");
					if (validationSet.numInstances() == 0) {
						featSelectWr.append("Not validation data for fold: "
								+ v);

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
							long trueLabelCfMIx = Math
									.round(vInst.classValue());
							long bestLabelInt = Math.round(vClass);
							foldFeactSelectCM[(int) trueLabelCfMIx]
									.addValue(bestLabelInt);
							synchronized (totalFeatSelectCM) {
								totalFeatSelectCM.get(attrSelectEvalClazz
										.getName() /*
													 * + searchClazz . getName
													 */)[(int) trueLabelCfMIx]
										.addValue(bestLabelInt);
							}
						}
						// a map of accuracies for different algot
						accuracyMap.put(
								attrSelectEvalClazz.getName(),
								featSelectCorrectCount * 1.0
										/ validationSet.numInstances());
						synchronized (cvFeatSelectAccuracyWr) {

							cvFeatSelectAccuracyWr
									.get(attrSelectEvalClazz.getName() /*
																		 * +
																		 * searchClazz
																		 * .
																		 * getName
																		 */)
									.append(Integer.toString(v))
									.append('\t')
									.append(Integer
											.toString(featSelectCorrectCount))
									.append('\t')
									.append(Integer.toString(validationSet
											.numInstances()))
									.append('\t')
									.append(Double.toString(accuracyMap
											.get(attrSelectEvalClazz.getName())))
									.append('\n');
						}

					}
				} catch (WekaException e) {
					if (e.getMessage().startsWith(
							"Not enough training instances")) {
						featSelectWr.append(e.getMessage());
						continue;
					}
				} catch (Exception ignored) {
					ignored.printStackTrace(System.err);
					continue;
				} finally {
					featSelectWr.flush();
					featSelectWr.close();
				}

				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifier.getClass().getName(),
						attrSelectEvalClazz.getName(), "v" + v
								+ "_feat-selection.txt"), featSelector
						.toString());
				// algo name
				System.out.println(baseClassifierClazz.getSimpleName() + "/"
						+ attrSelectEvalClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished feature selection for fold: " + v);
			}
			// //////////////////////////////////////

			return accuracyMap;
		}

	}

	private String outputPath = "C:\\mdc-datasets\\weka\\validation";
	private String inPath = "C:\\mdc-datasets\\weka\\segmented_user";

	private final Frequency[] totalConfusionMatrix;
	private final HashMap<String, Frequency[]> totalFeatSelectCM;
	private long startTime = System.currentTimeMillis();

	private final int inFoldTestIx = new Random(System.currentTimeMillis())
			.nextInt(Config.VALIDATION_FOLD_WIDTH);

	private final Writer cvClassificationAccuracyWr;
	private final HashMap<String, Writer> cvFeatSelectAccuracyWr;

	private Class<? extends Classifier> baseClassifierClazz;

	public ClassifyAndFeatSelect(Class<? extends Classifier> baseClassifierClazz)
			throws IOException {
		this.baseClassifierClazz = baseClassifierClazz;

		totalConfusionMatrix = new Frequency[Config.NUM_LABELS_CONSIDERED];
		for (int i = 0; i < totalConfusionMatrix.length; ++i) {
			totalConfusionMatrix[i] = new Frequency();
		}
		cvClassificationAccuracyWr = Channels
				.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(outputPath,
										baseClassifierClazz.getName(),
										"classification-accuracy-cv.txt"))
								.getChannel(), Config.OUT_CHARSET);
		cvClassificationAccuracyWr.append("vFold").append('\t').append("trueN")
				.append('\t').append("totalN").append('\t').append("accuracy")
				.append('\n');

		totalFeatSelectCM = new HashMap<String, Frequency[]>();
		cvFeatSelectAccuracyWr = new HashMap<String, Writer>();
		for (@SuppressWarnings("rawtypes")
		Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
			Frequency[] freqArr = new Frequency[Config.NUM_LABELS_CONSIDERED];
			for (int i = 0; i < freqArr.length; ++i) {
				freqArr[i] = new Frequency();
			}
			totalFeatSelectCM.put(
					attrSelectEvalClazz.getName() /* + searchClazz */, freqArr);

			Writer writer = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath,
									baseClassifierClazz.getName(),
									attrSelectEvalClazz.getName() /*
																 * + searchClazz
																 */,
									"feat-selected-accuracy-cv.txt"))
							.getChannel(), Config.OUT_CHARSET);

			writer.append("vFold").append('\t').append("trueN").append('\t')
					.append("totalN").append('\t').append("accuracy")
					.append('\n');
			cvFeatSelectAccuracyWr
					.put(attrSelectEvalClazz.getName() /* + searchClazz */,
							writer);

		}

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

		// Naive Bayes
		ClassifyAndFeatSelect app = new ClassifyAndFeatSelect(
				NaiveBayesUpdateable.class);
		app.call();

		// C4.5 decision tree
		app = new ClassifyAndFeatSelect(J48.class);
		app.call();

		// Bayesian Logisitc Regression
		app = new ClassifyAndFeatSelect(BayesianLogisticRegression.class);
		app.call();

		// Bayes Net
		app = new ClassifyAndFeatSelect(BayesNet.class);
		app.call();

		// Logistic Regression
		app = new ClassifyAndFeatSelect(Logistic.class);
		app.call();

		// SVM
		app = new ClassifyAndFeatSelect(LibSVM.class);
		app.call();

		// By clustering
		app = new ClassifyAndFeatSelect(ClassificationViaClustering.class);
		app.call();

	}

	public Void call() throws Exception {
		this.acrossUsersClassify();

		return null;
	}

	public void acrossUsersClassify() throws Exception {
		SummaryStatistics accuracySummaryAllFeatures = new SummaryStatistics();
		HashMap<String, SummaryStatistics> accuracySummaryFeatSelected = new HashMap<String, SummaryStatistics>();
		try {
			for (Class clazz : attrSelectEvaluationClazzes) {
				accuracySummaryFeatSelected.put(clazz.getName(),
						new SummaryStatistics());
			}

			System.out.println(baseClassifierClazz.getSimpleName() + " - "
					+ new Date().toString()
					+ " (total): Validating with every user number "
					+ inFoldTestIx + " within every "
					+ Config.VALIDATION_FOLD_WIDTH + " users");
			ExecutorService foldExecutors = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			ArrayList<Future<HashMap<String, Double>>> foldFutures = new ArrayList<Future<HashMap<String, Double>>>();
			int numClassifyTasks = 0;
			for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
				FoldCallable FoldCallable = new FoldCallable(v);
				foldFutures.add(foldExecutors.submit(FoldCallable));
				++numClassifyTasks;
			}
			for (int i = 0; i < numClassifyTasks; ++i) {
				HashMap<String, Double> accuracies = foldFutures.get(i).get();
				accuracySummaryAllFeatures.addValue(accuracies.get(ALL_FEATS));
				for (Class clazz : attrSelectEvaluationClazzes) {
					accuracySummaryFeatSelected.get(clazz.getName()).addValue(
							accuracies.get(clazz.getName()));
				}
			}
			foldExecutors.shutdown();
			while (!foldExecutors.isTerminated()) {
				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime)
						+ " (total): shutting down");
				Thread.sleep(1000);
			}
		} finally {
			cvClassificationAccuracyWr.flush();
			cvClassificationAccuracyWr.close();
			for (Writer wr : cvFeatSelectAccuracyWr.values()) {
				wr.flush();
				wr.close();
			}
		}
		FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
				baseClassifierClazz.getName(), "accuracy-summary.txt"),
				accuracySummaryAllFeatures.toString());

		for (Class clazz : attrSelectEvaluationClazzes) {
			FileUtils
					.writeStringToFile(FileUtils.getFile(outputPath,
							baseClassifierClazz.getName(), clazz.getName(),
							"feat-selected_accuracy-summary.txt"),
							accuracySummaryFeatSelected.get(clazz.getName())
									.toString());
		}
		Writer totalConfusionWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath,
								baseClassifierClazz.getName(),
								"confusion-matrix.txt")).getChannel(),
				Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(totalConfusionWr, totalConfusionMatrix);
		} finally {
			totalConfusionWr.flush();
			totalConfusionWr.close();
		}

		for (@SuppressWarnings("rawtypes")
		Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
			Writer featSelectedConfusionWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath,
									baseClassifierClazz.getName(),
									attrSelectEvalClazz.getName(),
									/* searchClazz.getName() */
									"feat-selected-confusion-matrix.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				writeConfusionMatrix(
						featSelectedConfusionWr,
						totalFeatSelectCM.get(attrSelectEvalClazz.getName()/*
																			 * +
																			 * searchClazz
																			 * .
																			 * getName
																			 */));
			} finally {
				featSelectedConfusionWr.flush();
				featSelectedConfusionWr.close();
			}
		}
		System.out.println(baseClassifierClazz.getSimpleName() + " - "
				+ new Date().toString() + " (total): Done in "
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
