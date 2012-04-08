package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;
import uwaterloo.util.NotifyStream;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.ComplementNaiveBayes;
import weka.classifiers.bayes.DMNBtext;
import weka.classifiers.bayes.HNB;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.bayes.NaiveBayesMultinomial;
import weka.classifiers.functions.GaussianProcesses;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.meta.AdaBoostM1;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.trees.ADTree;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.clusterers.Clusterer;
import weka.clusterers.EM;
import weka.clusterers.RandomizableClusterer;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.AddID;
import weka.filters.unsupervised.attribute.Remove;

public class ClusterClassifyEM implements Callable<Void> {

	private static String outputPathBase = "C:\\mdc-datasets\\weka\\testing";
	private static String inPath = "C:\\mdc-datasets\\weka\\segmented_user\\ALL";

	// EM gets the correct number
	// private static final int NUM_CLUSTERS = 8;

	private Random rand = new Random(System.currentTimeMillis());

	private Classifier baseClassifier;

	private PrincipalComponents attrSelection;

	private boolean ignoreInstsWithMissingClass;
	private File dataDir;
	private boolean validate = true;
	private int numClusters;
	private final Class<? extends Classifier> baseClassifierClazz;

	private final String outputPath;

	public ClusterClassifyEM(int numClusters,
			Class<? extends Classifier> baseClassifierClazz) {
		this(numClusters, false, FileUtils.getFile(inPath), baseClassifierClazz);
	}

	public ClusterClassifyEM(int numClusters,
			boolean ignoreInstsWithMissingClass, File dataDir,
			Class<? extends Classifier> baseClassifierClazz) {
		this.baseClassifierClazz = baseClassifierClazz;
		outputPath = FilenameUtils.concat(outputPathBase,
				baseClassifierClazz.getName());
		this.numClusters = numClusters;
		this.ignoreInstsWithMissingClass = ignoreInstsWithMissingClass;
		this.dataDir = dataDir;
	}

	public Void call() {

		File[] arffFiles = dataDir.listFiles(new FilenameFilter() {

			public boolean accept(File arg0, String arg1) {
				return arg1.endsWith(".arff");
			}
		});

		SummaryStatistics correctInitialSummary = new SummaryStatistics();
		SummaryStatistics correctClusteredSummary = new SummaryStatistics();
		SummaryStatistics correctFinalSummary = new SummaryStatistics();
		SummaryStatistics changedToCorrectSummaryFinal = new SummaryStatistics();
		SummaryStatistics changedFrmCorrectSummaryFinal = new SummaryStatistics();
		SummaryStatistics numThatChangedSummaryFinal = new SummaryStatistics();
		int[][] totalConfusionMatrixInitial = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
		int[][] totalConfusionMatrixFinal = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
		SummaryStatistics changedToCorrectSummaryClustered = new SummaryStatistics();
		SummaryStatistics changedFrmCorrectSummaryClustered = new SummaryStatistics();
		SummaryStatistics numThatChangedSummaryClustered = new SummaryStatistics();
		int[][] totalConfusionMatrixClustered = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];

		for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
			int foldStartIx = v * Config.VALIDATION_FOLD_WIDTH;
			File[] testFiles = Arrays.copyOfRange(arffFiles, foldStartIx,
					foldStartIx + Config.VALIDATION_FOLD_WIDTH);
			File[] trainingFiles = new File[Config.NUM_USERS_TO_PROCESS
					- Config.VALIDATION_FOLD_WIDTH];
			int t = 0;
			for (int f = 0; f < arffFiles.length; ++f) {
				if (f >= foldStartIx
						&& f < foldStartIx + Config.VALIDATION_FOLD_WIDTH) {
					continue;
				}
				trainingFiles[t] = arffFiles[f];
				++t;
				if (t == trainingFiles.length) {
					break;
				}
			}
			try {
				this.train(trainingFiles);
				for (int u = 0; u < Config.VALIDATION_FOLD_WIDTH; ++u) {

					Instances userInsts = new Instances(Channels.newReader(
							FileUtils.openInputStream(testFiles[u])
									.getChannel(), Config.OUT_CHARSET));
					String userId = userInsts.relationName();

					userInsts.setClassIndex(userInsts.numAttributes() - 1);
					// Not needed and causes trouble coz the dataset link is
					// lost
					// (right?)
					// userInsts.deleteWithMissingClass();

					UserPrediction uesrPrediction = this.predict(userInsts);

					File userResultFile = FileUtils.getFile(outputPath, "n"
							+ numClusters + "_" + userId + ".txt");
					FileUtils.writeStringToFile(userResultFile,
							uesrPrediction.toString());

					if (validate) {
						int confusionMatrixInitial[][] = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
						int confusionMatrixClustered[][] = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
						int confusionMatrixFinal[][] = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
						int correctFinal = 0;
						int correctClustered = 0;
						int correctInit = 0;
						int changedToCorrectFinal = 0;
						int changedFromCorrectFinal = 0;
						int totalChangedFinal = 0;
						int changedToCorrectClustered = 0;
						int changedFromCorrectClustered = 0;
						int totalChangedClustered = 0;
						Enumeration instEnum = userInsts.enumerateInstances();
						while (instEnum.hasMoreElements()) {
							Instance inst = (Instance) instEnum.nextElement();
							// String actualLabel =
							// inst.attribute(inst.numAttributes()-1).value((int)
							// Math.round(inst.value(inst.numAttributes()-1)));
							Integer actualLabel = (int) Math.round(inst
									.value(inst.numAttributes() - 1));
							Integer initialLabel = uesrPrediction.instInitialLabelMap
									.get(inst.value(0));//FIXME
							Integer finalLabel = uesrPrediction.instFinalLabelMap
									.get(inst.value(0));//FIXME
							Integer clusteredLabel = uesrPrediction.instClusteredLabelMap
									.get(inst.value(0));//FIXME
							if (finalLabel.equals(actualLabel)) {
								++correctFinal;
								if (!initialLabel.equals(finalLabel)) {
									++changedToCorrectFinal;
								}
							}
							++confusionMatrixFinal[actualLabel][finalLabel];
							++totalConfusionMatrixFinal[actualLabel][finalLabel];
							if (clusteredLabel.equals(actualLabel)) {
								++correctClustered;
								if (!initialLabel.equals(clusteredLabel)) {
									++changedToCorrectClustered;
								}
							}
							++confusionMatrixClustered[actualLabel][clusteredLabel];
							++totalConfusionMatrixClustered[actualLabel][clusteredLabel];

							if (initialLabel.equals(actualLabel)) {
								++correctInit;
								if (!initialLabel.equals(finalLabel)) {
									++changedFromCorrectFinal;
								}
								if (!initialLabel.equals(clusteredLabel)) {
									++changedFromCorrectClustered;
								}
							}
							++confusionMatrixInitial[actualLabel][initialLabel];
							++totalConfusionMatrixInitial[actualLabel][initialLabel];

							if (!initialLabel.equals(finalLabel)) {
								++totalChangedFinal;
							}
							if (!initialLabel.equals(clusteredLabel)) {
								++totalChangedClustered;
							}
						}

						correctInitialSummary.addValue(correctInit * 1.0
								/ userInsts.numInstances());
						correctFinalSummary.addValue(correctFinal * 1.0
								/ userInsts.numInstances());
						correctClusteredSummary.addValue(correctClustered * 1.0
								/ userInsts.numInstances());
						changedToCorrectSummaryFinal
								.addValue(changedToCorrectFinal * 1.0
										/ totalChangedFinal);
						changedFrmCorrectSummaryFinal
								.addValue(changedFromCorrectFinal * 1.0
										/ totalChangedFinal);
						numThatChangedSummaryFinal.addValue(totalChangedFinal);
						changedToCorrectSummaryClustered
								.addValue(changedToCorrectClustered * 1.0
										/ totalChangedClustered);
						changedFrmCorrectSummaryClustered
								.addValue(changedFromCorrectClustered * 1.0
										/ totalChangedClustered);
						numThatChangedSummaryClustered
								.addValue(totalChangedClustered);

						Writer userWr = Channels.newWriter(FileUtils
								.openOutputStream(userResultFile, true)
								.getChannel(), Config.OUT_CHARSET);
						try {
							userWr.append(
									"\n\nChanged To Correct Label Clustered: ")
									.append(Integer
											.toString(changedToCorrectClustered))
									.append('\n');
							userWr.append(
									"\nChanged FROM Correct Label Clustered: ")
									.append(Integer
											.toString(changedFromCorrectClustered))
									.append('\n');
							userWr.append("\nNum changed Clustered: ").append(
									Integer.toString(totalChangedClustered));

							userWr.append(
									"\n\nChanged To Correct Label Final: ")
									.append(Integer
											.toString(changedToCorrectFinal))
									.append('\n');
							userWr.append(
									"\nChanged FROM Correct Label Final: ")
									.append(Integer
											.toString(changedFromCorrectFinal))
									.append('\n');
							userWr.append("\nNum changed Final: ").append(
									Integer.toString(totalChangedFinal));

							userWr.append("\nTotal Instances: ")
									.append(Integer.toString(userInsts
											.numInstances())).append("\n\n");

							userWr.append("Initial correct classifications: ")
									.append(Integer.toString(correctInit))
									.append("\n\nInitial Confusion Matrix:\n");
							writeConfusionMatrix(userWr, confusionMatrixInitial);

							userWr.append(
									"\n\nClustered correct classifications: ")
									.append(Integer.toString(correctClustered))
									.append("\n\nClustered Confusion Matrix:\n");
							writeConfusionMatrix(userWr,
									confusionMatrixClustered);

							userWr.append("\n\nFinal correct classifications: ")
									.append(Integer.toString(correctFinal))
									.append("\n\nFinal Confusion Matrix:\n");
							writeConfusionMatrix(userWr, confusionMatrixFinal);

						} finally {
							userWr.flush();
							userWr.close();
						}
					}
				}
			} catch (Exception ignored) {
				ignored.printStackTrace();
			}

		}

		File summaryFile = FileUtils.getFile(outputPath, "n" + numClusters
				+ "_" + "summary.txt");
		Writer summaryWr = null;
		try {
			summaryWr = Channels.newWriter(
					FileUtils.openOutputStream(summaryFile).getChannel(),
					Config.OUT_CHARSET);
			summaryWr
					.append("Number of labels that changed due to clustering Summary (across users):\n")
					.append(numThatChangedSummaryClustered.toString())
					.append("\n\n");

			summaryWr
					.append("Number of labels that changed due to EM Summary (across users):\n")
					.append(numThatChangedSummaryFinal.toString())
					.append("\n\n");
			summaryWr
					.append("Pct of changes TO CORRECT due to clustering Summary (across users):\n")
					.append(changedToCorrectSummaryClustered.toString())
					.append("\n\n");

			summaryWr
					.append("Pct of changes TO CORRECT due to EM Summary (across users):\n")
					.append(changedToCorrectSummaryFinal.toString())
					.append("\n\n");

			summaryWr
					.append("Pct of changes FROM CORRECT due to clustering Summary (across users):\n")
					.append(changedFrmCorrectSummaryClustered.toString())
					.append("\n\n");

			summaryWr
					.append("Pct of changes FROM CORRECT due toEM Summary (across users):\n")
					.append(changedFrmCorrectSummaryFinal.toString())
					.append("\n\n");

			summaryWr.append("Initial accuracy Summary (across users):\n")
					.append(correctInitialSummary.toString()).append("\n\n");

			summaryWr.append("Clustered accuracy Summary (across users):\n")
					.append(correctClusteredSummary.toString()).append("\n\n");

			summaryWr.append("Final accuracy Summary (across users):\n")
					.append(correctFinalSummary.toString()).append("\n\n");

			summaryWr.append("Total confusion matrix INITIAL:\n");
			writeConfusionMatrix(summaryWr, totalConfusionMatrixInitial);

			summaryWr.append("\n\nTotal confusion matrix Clustered:\n");
			writeConfusionMatrix(summaryWr, totalConfusionMatrixClustered);

			summaryWr.append("\n\nTotal confusion matrix FINAL:\n");
			writeConfusionMatrix(summaryWr, totalConfusionMatrixFinal);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (summaryWr != null) {
					summaryWr.flush();
					summaryWr.close();
				}
			} catch (IOException e) {

				e.printStackTrace();
			}
		}

		return null;
	}

	public static void writeConfusionMatrix(Writer confusionWr,
			int[][] confusionMatrix) throws IOException {
		if (confusionMatrix.length == 0) {
			confusionWr.append("EMPTY");
			return;
		}

		confusionWr.append("label\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\n");

		for (int i = 0; i < confusionMatrix.length; ++i) {
			confusionWr.append(Integer.toString(i));
			long totalCount = 0;

			for (int j = 0; j < Config.LABELS_SINGLES.length; ++j) {
				long cnt = confusionMatrix[i][j]; // valsIster.next());
				totalCount += cnt;
				confusionWr.append('\t').append(Long.toString(cnt));
			}

			confusionWr.append('\t').append(Long.toString(totalCount))
					.append('\n');
		}
	}

	public UserPrediction predict(Instances testSet) throws Exception {
		// if (validate) {
		// // FIXMENOT: I don't know how some data still have missing class
		// // Avoid having actual class == -1.. they don't
		// testSet.deleteWithMissingClass();
		// }

		if (Config.CLUSTER_CLASSIFY_REM_PREVLABEL) {
			// Remove remPrev = new Remove();
			// remPrev.setInputFormat(testSet);
			// int numAttrs = testSet.numAttributes();
			// remPrev.setAttributeIndices((numAttrs - 11) + "-" + (numAttrs));
			// testSet = Filter.useFilter(testSet, remPrev);
			int delAttrIx = testSet.numAttributes() - 12;
			for (int a = 0; a < 11; ++a) {
				testSet.deleteAttributeAt(delAttrIx);
			}
		}

		Instances transSet = null;
		Instances noClassOrig;
		if (Config.CLUSTERCLASSIFY_CLUSTER_IN_DIMREDUCED) {
			transSet = attrSelection.transformedData(testSet);
			AddID addid = new AddID();
			addid.setAttributeName("ID");
			addid.setInputFormat(transSet);
			noClassOrig = Filter.useFilter(transSet, addid);
		} else {
			noClassOrig = testSet;
		}

		// This doesn't remove and I don't know why!
		Remove remClass = new Remove();
		remClass.setInputFormat(noClassOrig);
		remClass.setAttributeIndices("last"); //
		Integer.toString(noClassOrig.numAttributes());
		Instances noClassSet = Filter.useFilter(noClassOrig, remClass);
		// Work around:
		if (noClassSet.attribute(noClassSet.numAttributes() - 1).name()
				.equals("label")) {
			noClassSet.setClassIndex(-1);
			noClassSet.deleteAttributeAt(noClassSet.numAttributes() - 1);
		}

		transSet = null;

		Clusterer clusterer = null;
		double[] clusterSizes = null;
		double[] clusterPriors = null;
		boolean useSizeAsWeigtht = true;
		if (numClusters > 0) {
			// run k means a few times and choose best solution
			double bestSqE = Double.MAX_VALUE;
			for (int i = 0; i < Config.CLUSTERCLASSIFY_NUM_KMEAN_RUNS; i++) {
				Clusterer sk = new SimpleKMeans();
				if (sk instanceof RandomizableClusterer) {
					((RandomizableClusterer) sk).setSeed(rand.nextInt());
				}
				// if (sk instanceof NumberOfClustersRequestable) {
				((SimpleKMeans) sk).setNumClusters(numClusters);

				// sk.setDisplayStdDevs(true);
				// sk.setDistanceFunction(new ManhattanDistance(noClassSet));
				sk.buildClusterer(noClassSet);
				if (sk instanceof SimpleKMeans) {
					if (((SimpleKMeans) sk).getSquaredError() < bestSqE) {
						bestSqE = ((SimpleKMeans) sk).getSquaredError();
						clusterer = sk;
					}
				} else {
					clusterer = sk;
					break;
				}
			}

			// The idea was to sample the instances starting from centroids
			// Enumeration centroidEmum =
			// kMeans.getClusterCentroids().enumerateInstances();
			// while(centroidEmum.hasMoreElements()){
			// Instance centroid = (Instance) centroidEmum.nextElement();
			// // cannot get the closest neighbour
			// }
		} else {
			// Should provice better clusters (right?), but too slow
			EM em = new EM();
			clusterer = em;
			em.setSeed(rand.nextInt());
			em.buildClusterer(noClassSet);
			numClusters = em.numberOfClusters();
			// clusterPriors = new double[numClusters];
			clusterPriors = em.clusterPriors();
			useSizeAsWeigtht = false;
			//
			// XMeans xm = new XMeans();
			// clusterer = xm;
			// xm.setSeed(rand.nextInt());
			// xm.buildClusterer(noClassSet);
			// min and max clusters
			// numClusters = xm.numberOfClusters();
			// clusterWeights = new double[numClusters];
		}
		clusterSizes = new double[numClusters];

		Frequency[] clusterLabelPriors = new Frequency[numClusters];
		for (int c = 0; c < clusterLabelPriors.length; ++c) {
			clusterLabelPriors[c] = new Frequency();
			if (Config.CLUSTERCLASSIFY_LAPALCAE_SMOOTH_LABEL_PROBS) {
				// Laplace way of smoothing (i.e. prevent 0 prob)
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					clusterLabelPriors[c].addValue(l);
				}
			}
		}
		// long[][] clusterLabelCounts = new
		// long[numClusters][Config.LABELS_SINGLES.length];
		// // TODO Laplace

		TreeMap<Double, double[]> instPredictionMap = new TreeMap<Double, double[]>();
		TreeMap<Double, Integer> instInitialLabelMap = new TreeMap<Double, Integer>();
		TreeMap<Double, Integer> instClusterMap = new TreeMap<Double, Integer>();
		TreeMap<Double, Integer> instTrueLabelMap = null;
		if (validate) {
			instTrueLabelMap = new TreeMap<Double, Integer>();
		}
		Frequency predictiedLabelDistrib = new Frequency();

		// double[][] metricPerLabelForCluster = new
		// double[numClusters][Config.LABELS_SINGLES.length];
		Enumeration instEnum;
		// = testSet.enumerateInstances();
		// while (instEnum.hasMoreElements()) {
		int prevPredictedLabel = -1;
		for (int i = 0; i < testSet.numInstances(); ++i) {
			Instance testInst = testSet.instance(i);

			if (validate) {
				//FIXME
				instTrueLabelMap.put(testInst.value(0), (int) Math
						.round(testInst.value(testInst.numAttributes() - 1)));
			}

			double[] labelDistribution = new double[Config.LABELS_SINGLES.length];
			for (int l = 0; l < labelDistribution.length; ++l) {
				labelDistribution[l] = Double.NaN;
			}

			if (!Config.CLUSTER_CLASSIFY_REM_PREVLABEL) {
				int prevLabelStartIx = testInst.numAttributes()
						- labelDistribution.length - 1;
				if (Config.CLUSTER_CLASSIFY_PREVLABEL_DISTRIB) {
					for (int l = 0; l < labelDistribution.length; ++l) {
						testInst.setValue(prevLabelStartIx + l,
								labelDistribution[l]);
					}
				} else {
					testInst.setValue(prevLabelStartIx + prevPredictedLabel, 1);
				}

			}
			int predictedLabel = -1;
			double predictionProb = Double.NEGATIVE_INFINITY;
			Instance classMissingInst;

			if (validate) {
				classMissingInst = (Instance) testInst.copy();
				classMissingInst.setClassMissing();
			} else {
				classMissingInst = testInst;
			}

			labelDistribution = baseClassifier
					.distributionForInstance(classMissingInst);
			for (int l = 0; l < labelDistribution.length; ++l) {
				if (labelDistribution[l] > predictionProb) {
					predictionProb = labelDistribution[l];
					predictedLabel = l;
				} else if (labelDistribution[l] == predictionProb) {
					// randomly break tie
					if (rand.nextBoolean()) {
						predictionProb = labelDistribution[l];
						predictedLabel = l;

					}
				}
			}

			if (predictedLabel == -1) {
				// this never happens.. just a sanity check
				System.err.println("Cannot classify instance: "
						+ testInst.toString());
				predictedLabel = 0;
			}
			prevPredictedLabel = predictedLabel;
			predictiedLabelDistrib.addValue(predictedLabel);
			Instance noClassInst = noClassSet.instance(i);
			int c = clusterer.clusterInstance(noClassInst);
			instClusterMap.put(noClassInst.value(0), c);//FIXME
			++clusterSizes[c];

			clusterLabelPriors[c].addValue(predictedLabel);
			instPredictionMap.put(noClassInst.value(0), // ID//FIXME
					labelDistribution);
			instInitialLabelMap.put(noClassInst.value(0), // ID//FIXME
					// inst.attribute(inst.numAttributes() - 1).value(
					// predictedLabel));
					predictedLabel);

			// for (int l = 0; l < labelDistribution.length; ++l) {
			// if (labelDistribution[l] > 0) {
			// if (Config.CLUSTER_CLASSIFY_METRIC
			// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
			// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE) {
			// metricPerLabelForCluster[c][l] += Math
			// .log(labelDistribution[l]);
			// } else {
			// metricPerLabelForCluster[c][l] *= labelDistribution[l];
			// }
			// } else if (Config.CLUSTER_CLASSIFY_METRIC
			// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.ENTROPY)) {
			// if (labelDistribution[l] != 0) {
			// metricPerLabelForCluster[c][l] += labelDistribution[l]
			// * Math.log(1 / labelDistribution[l]);
			// }
			// }
			//
			// }
			// }
		}
		// Normalize cluster weights
		if (useSizeAsWeigtht) {
			clusterPriors = Arrays.copyOf(clusterSizes, numClusters);
			for (int c = 0; c < clusterPriors.length; ++c) {
				clusterPriors[c] /= testSet.numInstances();
			}
		}

		// // Keep a copy for history.. a shallow copy is enough since they
		// // are updated by creating new objects
		LinkedList<Integer>[] clusterLabelsClustered = assignClusterLabels(clusterLabelPriors);
		TreeMap<Double, Integer> instClusteredLabelMap = new TreeMap<Double, Integer>();
		instEnum = testSet.enumerateInstances();
		while (instEnum.hasMoreElements()) {
			Instance inst = (Instance) instEnum.nextElement();
			int c = instClusterMap.get(inst.value(0));

			Integer label = instInitialLabelMap.get(inst.value(0));
			if (!clusterLabelsClustered[c].contains(label)) {
				label = clusterLabelsClustered[c].get(rand
						.nextInt(clusterLabelsClustered[c].size()));
			}
			instClusteredLabelMap.put(inst.value(0), label);

		}

		Frequency[] clusterLabelFinalFrequency = Arrays.copyOf(
				clusterLabelPriors, numClusters);
		// TreeMap<Double, Integer> instFinalLabelMap = new TreeMap<Double,
		// Integer>();
		// instFinalLabelMap.putAll(instInitialLabelMap);

		Frequency marginalLabelProbabilitGivenAssignment = predictiedLabelDistrib;

		LinkedList<Integer>[] clusterLabelsFinalAssignment = null;

		// Maximize expectation of clusters label assignment
		double[][] emWeights = new double[numClusters][Config.LABELS_SINGLES.length];
		double expectation = 0, expectationOld;

		// For debug only
		boolean doMaximizationAndReportingThenBreak = false;

		for (int i = 0; i < Config.CLUSTERCALSSIFY_LABEL_ASSG_MAX_ITERS; ++i) {

			StringBuffer report = new StringBuffer();
			report.append("=============================\n")
					.append("Iteration: " + i).append('\n')
					.append("Expectation: " + expectation).append('\n')
					.append("C");
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				report.append('\t').append("L" + l);
			}
			report.append('\n');
			for (int c = 0; c < numClusters; ++c) {
				report.append(c);
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					report.append('\t').append(
							StringUtils.limitLength(Double
									.toString(clusterLabelFinalFrequency[c]
											.getPct(l)), 5));
				}
				report.append('\n');

			}
			report.append(
					"Label Marginal probabilities given assigntment:\n"
							+ marginalLabelProbabilitGivenAssignment.toString())
					.append('\n');
			System.out.println(report.toString());

			if (doMaximizationAndReportingThenBreak) {
				break;
			}

			// Prob that label is j, given that the cluster label is i
			double[][] jointPredictedLabelClusterLabel = new double[numClusters][Config.LABELS_SINGLES.length];

			double logLkhood = 0.0;
			double sumOfWeights = 0.0;
			instEnum = testSet.enumerateInstances();
			clusterLabelsFinalAssignment = assignClusterLabels(clusterLabelFinalFrequency);

			while (instEnum.hasMoreElements()) {
				Instance inst = (Instance) instEnum.nextElement();
				int c = instClusterMap.get(inst.value(0));
				double[] instLabelDistrib = instPredictionMap
						.get(inst.value(0));
				// LinkedList<Integer> clusterLabels =
				// clusterLabelsFinalAssignment[c];

				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					if (instLabelDistrib[l] > 0
							&& clusterLabelFinalFrequency[c].getPct(l) > 0) {
						jointPredictedLabelClusterLabel[c][l] += Math
								.log(clusterLabelFinalFrequency[c].getPct(l))
								+ Math.log(instLabelDistrib[l]);
					}
				}
			}

			// double[][] conditionalPredictedLabelCluster = new
			// double[numClusters][Config.LABELS_SINGLES.length];

			for (int c = 0; c < numClusters; ++c) {
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					double conditionalPredictedLabelCluster = clusterLabelFinalFrequency[c]
							.getCount(l) * 1.0 / testSet.numInstances();
					double weight = 1;
					// marginalLabelProbabilitGivenAssignment
					// .getPct(l) * clusterPriors[c];
					double term = conditionalPredictedLabelCluster
							* jointPredictedLabelClusterLabel[c][l];
					emWeights[c][l] = (1 - term);
					logLkhood += weight * term;
					sumOfWeights += weight;
				}
				MathUtil.normalizeProbabilities(emWeights[c]);
			}

			expectationOld = expectation;
			expectation = logLkhood / sumOfWeights;

			if (i > 0) {
				if ((expectation - expectationOld) < Config.CLUSTERCLASSIFY_EPSILON) {
					break; // for debugging:
					// doMaximizationAndReportingThenBreak
					// = true;
				}
			}

			// Restimate the label probabilities (M step in EM)
			marginalLabelProbabilitGivenAssignment = new Frequency();
			clusterLabelFinalFrequency = new Frequency[numClusters];
			for (int c = 0; c < clusterLabelFinalFrequency.length; ++c) {
				clusterLabelFinalFrequency[c] = new Frequency();
				if (Config.CLUSTERCLASSIFY_LAPALCAE_SMOOTH_LABEL_PROBS) {
					// Laplace way of smoothing (i.e. prevent 0 prob)
					for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
						clusterLabelFinalFrequency[c].addValue(l);
					}
				}
			}

			instEnum = testSet.enumerateInstances();
			while (instEnum.hasMoreElements()) {
				Instance inst = (Instance) instEnum.nextElement();
				int c = instClusterMap.get(inst.value(0));

				double[] labelDistrib = instPredictionMap.get(inst.value(0));
				double predictionProb = Double.NEGATIVE_INFINITY;
				int predictedLabel = -1;
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					labelDistrib[l] *= emWeights[c][l];
					if ((labelDistrib[l] > predictionProb)
							|| ((labelDistrib[l] > predictionProb) && rand
									.nextBoolean())) {
						predictionProb = labelDistrib[l];
						predictedLabel = l;
					}
				}
				marginalLabelProbabilitGivenAssignment.addValue(predictedLabel);
				MathUtil.normalizeProbabilities(labelDistrib);
				instPredictionMap.put(inst.value(0), labelDistrib);

				if (predictedLabel == -1) {
					// this never happens.. just a sanity check
					System.err.println("Cannot classify instance: "
							+ inst.toString());
					predictedLabel = 0;
				}

				clusterLabelFinalFrequency[c].addValue(predictedLabel);
			}

		}

		// clusterLabelsFinalAssignment =
		// assignClusterLabels(clusterLabelFinalFrequency);
		marginalLabelProbabilitGivenAssignment = new Frequency();
		clusterLabelFinalFrequency = new Frequency[numClusters];
		for (int c = 0; c < clusterLabelFinalFrequency.length; ++c) {
			clusterLabelFinalFrequency[c] = new Frequency();
			if (Config.CLUSTERCLASSIFY_LAPALCAE_SMOOTH_LABEL_PROBS) {
				// Laplace way of smoothing (i.e. prevent 0 prob)
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					clusterLabelFinalFrequency[c].addValue(l);
				}
			}
		}

		TreeMap<Double, Integer> instFinalLabelMap = new TreeMap<Double, Integer>();
		double[] clusterPurity = new double[numClusters]; // [Config.LABELS_SINGLES.length];

		instEnum = testSet.enumerateInstances();
		while (instEnum.hasMoreElements()) {
			Instance inst = (Instance) instEnum.nextElement();
			int c = instClusterMap.get(inst.value(0));

			Integer label = instInitialLabelMap.get(inst.value(0));
			if (!clusterLabelsFinalAssignment[c].contains(label)) {
				label = clusterLabelsFinalAssignment[c].get(rand
						.nextInt(clusterLabelsFinalAssignment[c].size()));
			} else {
				++clusterPurity[c];
			}
			instFinalLabelMap.put(inst.value(0), label);
			clusterLabelFinalFrequency[c].addValue(label.intValue());
			marginalLabelProbabilitGivenAssignment.addValue(label.intValue());
		}
		// long[] clusterLabels = new long[numClusters];
		// for (int c = 0; c < clusterLabels.length; ++c) {
		// Iterator<Comparable<?>> valsIter = clusterLabelFreq[c]
		// .valuesIterator();
		// double lPct = Double.NEGATIVE_INFINITY;
		// while (valsIter.hasNext()) {
		// Long l = (Long) valsIter.next();
		// if (clusterLabelFreq[c].getPct(l) > lPct
		// || (clusterLabelFreq[c].getPct(l) == lPct && rand
		// .nextBoolean())) {
		// clusterLabels[c] = l;
		// lPct = clusterLabelFreq[c].getPct(l);
		// }
		// }
		// }

		for (int c = 0; c < numClusters; ++c) {
			clusterPurity[c] /= clusterSizes[c];
		}

		UserPrediction result = new UserPrediction();
		result.numClusters = numClusters;
		result.instFinalLabelMap = instFinalLabelMap;
		result.instClusteredLabelMap = instClusteredLabelMap;
		result.instInitialLabelMap = instInitialLabelMap;
		result.instClusterMap = instClusterMap;
		result.clusterLabelsFinal = clusterLabelsFinalAssignment;
		result.clusterLabelsClustered = clusterLabelsClustered;
		result.clusterLabelFreq = clusterLabelFinalFrequency;
		result.clusterLabelPriors = clusterLabelPriors;
		result.instTrueLabelMap = instTrueLabelMap;
		result.clusterPurityFinal = clusterPurity;

		return result;
	}

	private LinkedList<Integer>[] assignClusterLabels(
			Frequency[] clusterLabelFreq) {
		double[][] clusterLabelDistrib = new double[numClusters][Config.LABELS_SINGLES.length];
		for (int c = 0; c < numClusters; ++c) {
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				clusterLabelDistrib[c][l] = clusterLabelFreq[c].getPct(l);
			}
		}
		return assignClusterLabels(clusterLabelDistrib);
	}

	private LinkedList<Integer>[] assignClusterLabels(
			double[][] clusterLabelFreq) {
		LinkedList<Integer>[] clusterLabels = new LinkedList[numClusters];
		for (int c = 0; c < clusterLabels.length; ++c) {
			clusterLabels[c] = new LinkedList<Integer>();
			double lPct = clusterLabelFreq[c][0];
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				// Iterator<Comparable<?>> valsIter = clusterLabelFreq[c]
				// .valuesIterator();
				// clusterLabels[c].add(((Long) valsIter.next()).intValue());
				// double lPct =
				// clusterLabelFreq[c].getPct(clusterLabels[c].get(0));
				// while (valsIter.hasNext()) {
				// Long l = (Long) valsIter.next();
				double probDiv = clusterLabelFreq[c][l] / lPct;
				if (probDiv > 1) {
					if (probDiv > Config.CLASSIFY_CLUSTER_DEFINITIVE_PROB_DIV) {
						// This is a pure cluster
						clusterLabels[c].clear();
					}
					clusterLabels[c].add(l);
					lPct = clusterLabelFreq[c][l];
				} else if (Math.abs(clusterLabelFreq[c][l] - lPct) < Config.CLASSIFY_CLUSTER_SMALL_PROB_DIFF) {
					// This cluster is not pure
					clusterLabels[c].add(l);
					// lPct = clusterLabelFreq[c].getPct(l);
				}
			}
		}
		return clusterLabels;
	}

	public class UserPrediction {
		public double[] clusterPurityFinal;
		public TreeMap<Double, Integer> instTrueLabelMap;
		public int numClusters;
		public TreeMap<Double, Integer> instClusterMap;
		public Frequency[] clusterLabelPriors;
		public Frequency[] clusterLabelFreq;
		public LinkedList<Integer>[] clusterLabelsFinal;
		public LinkedList<Integer>[] clusterLabelsClustered;
		public TreeMap<Double, Integer> instInitialLabelMap;
		public TreeMap<Double, Integer> instFinalLabelMap;
		public TreeMap<Double, Integer> instClusteredLabelMap;

		@Override
		public String toString() {

			StringBuffer result = new StringBuffer();
			result.append("Cluster Prior Label Distrib:\n")
					.append("=================\n").append("Cluster");
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				result.append('\t').append(l);

			}
			result.append('\n');
			for (int c = 0; c < numClusters; ++c) {
				result.append(c);
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					result.append('\t').append(
							Double.toString(clusterLabelPriors[c].getPct(l)));

				}
				result.append('\n');
			}
			result.append('\n');

			result.append("Cluster Final Label Distrib:\n")
					.append("=================\n").append("Cluster");
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				result.append('\t').append(l);

			}
			result.append('\n');
			for (int c = 0; c < numClusters; ++c) {
				result.append(c);
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					result.append('\t').append(
							Double.toString(clusterLabelFreq[c].getPct(l)));

				}
				result.append('\n');
			}
			result.append('\n');

			result.append("\nCluster\tinitialLabelling\tfinalLabelling\n");
			for (int c = 0; c < numClusters; ++c) {
				result.append(c)
						.append('\t')
						.append("{" + clusterLabelsClustered[c].toString()
								+ "}").append('\t')
						.append("{" + clusterLabelsFinal[c].toString() + "}")
						.append('\n');
			}

			result.append("Instances Clusters and Labels:\n")
					.append("====================================\n")
					.append("InstID\tCluster\tinitialLabel\tclusteredLabel\tfinalLabel");
			if (validate) {
				result.append("\ttrueLabel");
			}
			result.append('\n');

			int countChangedLabelsFinal = 0;
			int countChangedLabelsClustering = 0;
			for (Double instID : instClusterMap.keySet()) {
				result.append(instID).append('\t')
						.append(instClusterMap.get(instID)).append('\t')
						.append(instInitialLabelMap.get(instID)).append('\t')
						.append(instClusteredLabelMap.get(instID)).append('\t')
						.append(instFinalLabelMap.get(instID));
				if (validate) {
					result.append('\t').append(instTrueLabelMap.get(instID));
				}
				result.append('\n');

				if (instInitialLabelMap.get(instID) != instFinalLabelMap
						.get(instID)) {
					++countChangedLabelsFinal;
				}
				if (instInitialLabelMap.get(instID) != instClusteredLabelMap
						.get(instID)) {
					++countChangedLabelsClustering;
				}
			}
			result.append('\n');

			result.append("Instances That Changed Label after Clustering: ")
					.append(countChangedLabelsClustering)
					.append(" / ")
					.append(instClusterMap.keySet().size())
					.append(" = ")
					.append(countChangedLabelsClustering * 1.0
							/ instClusterMap.keySet().size()).append('\n');

			result.append("Instances That ULTIMATELY Changed Label: ")
					.append(countChangedLabelsFinal)
					.append(" / ")
					.append(instClusterMap.keySet().size())
					.append(" = ")
					.append(countChangedLabelsFinal * 1.0
							/ instClusterMap.keySet().size()).append('\n');

			result.append("\nPctg of intaces in each cluster which originally had the same label: "
					+ Arrays.toString(clusterPurityFinal));
			return result.toString();
		}
	}

	public void train(File[] trainingFiles) throws Exception {
		Instances trainingSet = null;

		int userIx = 0;
		for (File userData : trainingFiles) {
			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(userData);

			Instances dataStruct = dataLoader.getStructure();
			dataStruct.setClassIndex(dataStruct.numAttributes() - 1);

			if (userIx == 0) {
				trainingSet = new Instances(dataStruct);
			}

			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {
				if (ignoreInstsWithMissingClass && dataInst.classIsMissing()) {
					continue;
				}

				trainingSet.add(dataInst);
			}

			++userIx;
		}

		if (Config.CLUSTER_CLASSIFY_REM_PREVLABEL) {

			// Remove remPrev = new Remove();
			// remPrev.setInputFormat(trainingSet);
			// remPrev.setAttributeIndices((numAttrs - 11) + "-" + (numAttrs));
			// trainingSet = Filter.useFilter(trainingSet, remPrev);
			int delAttrIx = trainingSet.numAttributes() - 12;
			for (int a = 0; a < 11; ++a) {
				trainingSet.deleteAttributeAt(delAttrIx);
			}
		}

		baseClassifier = baseClassifierClazz.getConstructor().newInstance();
		baseClassifier.buildClassifier(trainingSet);

		attrSelection = new PrincipalComponents();
		ASSearch attrSearch = new Ranker();
		AttributeSelectedClassifier asClassifier = new AttributeSelectedClassifier();
		asClassifier.setClassifier(baseClassifier);
		asClassifier.setEvaluator(attrSelection);
		asClassifier.setSearch(attrSearch);
		asClassifier.buildClassifier(trainingSet);

		baseClassifier = asClassifier;

		trainingSet = null;
	}

	static Class<? extends Classifier>[] classifierClazzes = new Class[] { J48.class,
	// NaiveBayes.class,
	// RandomForest.class,
	// ComplementNaiveBayes.class,
	// AdaBoostM1.class,BayesNet.class, LibSVM.class,
	// MultilayerPerceptron.class
	// Cannot handle numeric attributes:
	// HNB.class,
	// Cannot handle multi-valued nominal class!
	// GaussianProcesses.class,
	// ADTree.class,
	// Cannot handle missing values! (do we have any??):
	// NaiveBayesMultinomial.class,
	// DMNBtext.class
	};

	public static void main(String[] args) throws InterruptedException,
			ExecutionException {

		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig,
				"ClusterClassifyEM");
		try {
			System.setErr(new PrintStream(notifyStream));

			ExecutorService diffClusterNumExecutor = Executors
					.newFixedThreadPool((Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MAX
							- Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MIN + 1)
							* classifierClazzes.length);
			// TODONOT: iterate over folds here, and train classifier once for
			// each
			// fold.. the classifier is not thread safe
			Future<Void> lastFuture = null;
			for (int c = Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MIN; c <= Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MAX; ++c) {
				for (Class clazz : classifierClazzes) {
					ClusterClassifyEM app = new ClusterClassifyEM(c, clazz);
					lastFuture = diffClusterNumExecutor.submit(app);
				}
			}

			// I bet their implementation of waiting is smarter
			// that the busy wait below (but don't remove it)
			lastFuture.get();

			diffClusterNumExecutor.shutdown();

			while (!diffClusterNumExecutor.isTerminated()) {
				Thread.sleep(5000);
			}
		} finally {
			try {
				notifyStream.close();
			} catch (IOException ignored) {

			}
			System.setErr(errOrig);
		}

		// How to save classifier while it is not serializable?
		// ObjectOutputStream classifierStreamer = new ObjectOutputStream(
		// FileUtils.openOutputStream(FileUtils.getFile(outputPath,
		// "classifier.bin")));
		// try {
		// classifierStreamer.writeObject(classifierStreamer);
		// } finally {
		// classifierStreamer.flush();
		// classifierStreamer.close();
		// }

		// I wanted to put this in predict, but clusterer is not serializable :(
		// File cachedClusterer = FileUtils.getFile(outputPath, "cache",
		// userId + "_clusterer.bin");
		//
		// if(cachedClusterer.exists()){
		// // Not serializable
		// }
		// // cache the clusterer
		// ObjectInputStream clustererStreamer = new ObjectInputStream(
		// FileUtils.openInputStream(cachedClusterer));
		// try {
		// clusterer = (Clusterer) clustererStreamer.readObject();
		// } finally {
		// clustererStreamer.flush();
		// clustererStreamer.close();
		// }
	}

	// private void claculateMessedStuff(double[][] emWeights, Object[]
	// clusterLabelsFinal){
	// double logLkhood = 0.0;
	// double sumOfWeights = 0.0;
	// if (!Config.CLUSTER_CLASSIFY_METRIC
	// .equals(Config.CLUSTER_CLASSIFY_METRIC_ENUM.ERROR)) {
	// double[] metricPerLabel = new double[Config.LABELS_SINGLES.length];
	// for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
	// double probLabel = marginalLabelProbabilit.getPct(l);
	// if (Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
	// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE) {
	// if (probLabel > 0) {
	// metricPerLabel[l] += Math.log(probLabel);
	// }
	// } else {
	// if (metricPerLabel[l] == 0) {
	// metricPerLabel[l] = 1;
	// }
	// metricPerLabel[l] *= probLabel;
	// }
	// } else if (Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.ENTROPY)) {
	//
	// if (probLabel != 0) {
	// metricPerLabel[l] += probLabel
	// * Math.log(1 / probLabel);
	// }
	// }
	// }
	//
	// // Estimate the labels of the clusters (E in the EM)
	//
	// for (int c = 0; c < numClusters; ++c) {
	//
	// // For this cluster, what is the joint probability
	// double[] logJointProbForCluster = new
	// double[Config.LABELS_SINGLES.length];
	// int maxLIx = 0;
	//
	// for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
	// double labelProbPerCluster = clusterLabelFreq[c]
	// .getPct(l);
	//
	// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE
	// && Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
	// if (labelProbPerCluster > 0) {
	// logJointProbForCluster[l] = metricPerLabel[l]
	// + Math.log(labelProbPerCluster);
	// }
	// } else {
	// if (logJointProbForCluster[l] == 0) {
	// logJointProbForCluster[l] = 1;
	// }
	// logJointProbForCluster[l] *= labelProbPerCluster;
	// }
	//
	// if (logJointProbForCluster[l] > logJointProbForCluster[maxLIx]) {
	// maxLIx = l;
	// }
	// }
	//
	// // MathUtil.normalizeProbabilities(logJointProbForCluster);
	//
	// double sum = 0.0;
	// for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
	//
	// if (Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
	// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE) {
	// sum += Math.exp(logJointProbForCluster[l]
	// - logJointProbForCluster[maxLIx]);
	// } else {
	// sum += logJointProbForCluster[l]
	// / logJointProbForCluster[maxLIx];
	// }
	// } else {
	// // if (Config.CLUSTER_CLASSIFY_METRIC
	// // .equals(CLUSTER_CLASSIFY_METRIC_ENUM.ENTROPY)) {
	// sum += logJointProbForCluster[l];
	// }
	//
	// }
	//
	// if (sum == 0) {
	// // FIXME: this cluster contains no instances???FI
	// // proceeding will lead to NaNs and exceptions :(
	// continue;
	// }
	//
	// // This is the argmax equation
	// double logProbForCluster = logJointProbForCluster[maxLIx];
	// if (Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
	// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE) {
	// logProbForCluster += Math.log(sum);
	// } else {
	// logProbForCluster *= sum;
	// }
	// } else {
	// // if (Config.CLUSTER_CLASSIFY_METRIC
	// // .equals(CLUSTER_CLASSIFY_METRIC_ENUM.ENTROPY)) {
	// logProbForCluster *= sum;
	// }
	//
	// // Add
	//
	// logLkhood += clusterWeights[c] * logProbForCluster;
	// sumOfWeights += clusterWeights[c];
	//
	// // Update weights
	// for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
	// if (Config.CLUSTER_CLASSIFY_METRIC
	// .equals(CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD)) {
	// if (Config.CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE) {
	// emWeights[c][l] = (Math
	// .exp(logJointProbForCluster[l]
	// - logJointProbForCluster[maxLIx]))
	// / sum;
	// } else {
	// emWeights[c][l] = logJointProbForCluster[l];
	// }
	// } else {
	// // if (Config.CLUSTER_CLASSIFY_METRIC
	// // .equals(CLUSTER_CLASSIFY_METRIC_ENUM.ENTROPY)) {
	// emWeights[c][l] = logJointProbForCluster[l] / sum;
	// }
	// }
	// }
	// } else {
	// // Minimize error
	// for (int c = 0; c < numClusters; ++c) {
	// double error = 0;
	// for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
	// if (clusterLabelsFinal[c].contains(l)) {
	// emWeights[c][l] = 1;
	// } else {
	// emWeights[c][l] = (1 - clusterLabelFreq[c]
	// .getPct(l))
	// * marginalLabelProbabilit.getPct(l);
	// }
	// error += 1 - emWeights[c][l];
	// }
	// logLkhood += clusterWeights[c] * error;
	// sumOfWeights += clusterWeights[c];
	// }
	// }
	//
	// }
}
