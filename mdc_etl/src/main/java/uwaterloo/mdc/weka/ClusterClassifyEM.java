package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.classifiers.Classifier;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.trees.J48;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

public class ClusterClassifyEM implements Callable<Void> {

	private static String outputPath = "C:\\mdc-datasets\\weka\\testing";
	private static String inPath = "C:\\mdc-datasets\\weka\\segmented_user\\ALL";

	// EM gets the correct number
	// private static final int NUM_CLUSTERS = 8;

	private Random rand = new Random(System.currentTimeMillis());

	private Classifier baseClassifier;

	private PrincipalComponents attrSelection;

	private boolean ignoreInstsWithMissingClass;
	private File dataDir;
	private boolean validate = true;
	private final int numClusters;

	public ClusterClassifyEM(int numClusters) {
		this(numClusters, false, FileUtils.getFile(inPath));
	}

	public ClusterClassifyEM(int numClusters,
			boolean ignoreInstsWithMissingClass, File dataDir) {
		this.numClusters = numClusters;
		this.ignoreInstsWithMissingClass = ignoreInstsWithMissingClass;
		this.dataDir = dataDir;
	}

	public Void call() throws Exception {

		File[] arffFiles = dataDir.listFiles(new FilenameFilter() {

			public boolean accept(File arg0, String arg1) {
				return arg1.endsWith(".arff");
			}
		});

		SummaryStatistics correctInitialSummary = new SummaryStatistics();
		SummaryStatistics correctFinalSummary = new SummaryStatistics();
		SummaryStatistics changedToCorrectSummary = new SummaryStatistics();
		SummaryStatistics changedFrmCorrectSummary = new SummaryStatistics();
		SummaryStatistics numThatChangedSummary = new SummaryStatistics();

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
			this.train(trainingFiles);
			for (int u = 0; u < Config.VALIDATION_FOLD_WIDTH; ++u) {

				Instances userInsts = new Instances(Channels.newReader(
						FileUtils.openInputStream(testFiles[u]).getChannel(),
						Config.OUT_CHARSET));
				String userId = userInsts.relationName();

				userInsts.setClassIndex(userInsts.numAttributes() - 1);
				// Not needed and causes trouble coz the dataset link is lost
				// (right?)
				// userInsts.deleteWithMissingClass();

				UserPrediction uesrPrediction = this.predict(userInsts);

				File userResultFile = FileUtils.getFile(outputPath, "n"
						+ numClusters + "_" + userId + ".txt");
				FileUtils.writeStringToFile(userResultFile,
						uesrPrediction.toString());

				if (validate) {
					int confusionMatrixInitial[][] = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
					int confusionMatrixFinal[][] = new int[Config.LABELS_SINGLES.length][Config.LABELS_SINGLES.length];
					int correctFinal = 0;
					int correctInit = 0;
					int changedToCorrect = 0;
					int changedFromCorrect = 0;
					int totalChanged = 0;
					Enumeration instEnum = userInsts.enumerateInstances();
					while (instEnum.hasMoreElements()) {
						Instance inst = (Instance) instEnum.nextElement();
						// String actualLabel =
						// inst.attribute(inst.numAttributes()-1).value((int)
						// Math.round(inst.value(inst.numAttributes()-1)));
						Integer actualLabel = (int) Math.round(inst.value(inst
								.numAttributes() - 1));
						Integer initialLabel = uesrPrediction.instInitialLabelMap
								.get(inst.value(0));
						Integer finalLabel = uesrPrediction.instFinalLabelMap
								.get(inst.value(0));
						if (finalLabel.equals(actualLabel)) {
							++correctFinal;
							if (!initialLabel.equals(finalLabel)) {
								++changedToCorrect;
							}
						}
						++confusionMatrixFinal[actualLabel][finalLabel];

						if (initialLabel.equals(actualLabel)) {
							++correctInit;
							if (!initialLabel.equals(finalLabel)) {
								++changedFromCorrect;
							}
						}
						++confusionMatrixInitial[actualLabel][initialLabel];

						if (!initialLabel.equals(finalLabel)) {
							++totalChanged;
						}
					}

					correctInitialSummary.addValue(correctInit * 1.0
							/ userInsts.numInstances());
					correctFinalSummary.addValue(correctFinal * 1.0
							/ userInsts.numInstances());
					changedToCorrectSummary.addValue(changedToCorrect * 1.0
							/ totalChanged);
					changedFrmCorrectSummary.addValue(changedFromCorrect * 1.0
							/ totalChanged);
					numThatChangedSummary.addValue(totalChanged);

					Writer userWr = Channels.newWriter(FileUtils
							.openOutputStream(userResultFile, true)
							.getChannel(), Config.OUT_CHARSET);
					try {
						userWr.append("\n\nChanged To Correct Label: ")
								.append(Integer.toString(changedToCorrect))
								.append('\n');
						userWr.append("\nChanged FROM Correct Label: ")
								.append(Integer.toString(changedFromCorrect))
								.append('\n');

						userWr.append("\nTotal Instances: ")
								.append(Integer.toString(userInsts
										.numInstances())).append("\n\n");

						userWr.append("Initial correct classifications: ")
								.append(Integer.toString(correctInit))
								.append("\n\nInitial Confusion Matrix:\n");
						writeConfusionMatrix(userWr, confusionMatrixInitial);

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
		}

		File summaryFile = FileUtils.getFile(outputPath, "n" + numClusters
				+ "_" + "summary.txt");
		Writer summaryWr = Channels.newWriter(
				FileUtils.openOutputStream(summaryFile).getChannel(),
				Config.OUT_CHARSET);
		try {
			summaryWr
					.append("Number of labels that changed due to clustering Summary (across users):\n")
					.append(numThatChangedSummary.toString()).append("\n\n");
			summaryWr
					.append("Pct of changes TO CORRECT due to clustering Summary (across users):\n")
					.append(changedToCorrectSummary.toString()).append("\n\n");
			summaryWr
					.append("Pct of changes FROM CORRECT due to clustering Summary (across users):\n")
					.append(changedFrmCorrectSummary.toString()).append("\n\n");
			summaryWr.append("Initial accuracy Summary (across users):\n")
					.append(correctInitialSummary.toString()).append("\n\n");
			summaryWr.append("Final accuracy Summary (across users):\n")
					.append(correctFinalSummary.toString()).append("\n\n");

		} finally {
			summaryWr.flush();
			summaryWr.close();
		}

		return null;
	}

	public static void writeConfusionMatrix(Writer confusionWr,
			int[][] confusionMatrix) throws IOException {
		if (confusionMatrix.length == 0) {
			confusionWr.append("EMPTY");
			return;
		}

		confusionWr.append("label\t0\t1t\2\t3\t4\t5\t6\t7\t8\t9\t10\n");

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

		// This doesn't remove and I don't know why!
		Remove remClass = new Remove();
		remClass.setInputFormat(testSet);
		remClass.setAttributeIndices("last"); // Integer.toString(testSet.numAttributes());
		Instances noClassSet = Filter.useFilter(testSet, remClass);
		noClassSet.setClassIndex(-1);

		// run k means a few times and choose best solution
		SimpleKMeans clusterer = null;
		double bestSqE = Double.MAX_VALUE;
		for (int i = 0; i < Config.CLUSTERCLASSIFY_NUM_KMEAN_RUNS; i++) {
			SimpleKMeans sk = new SimpleKMeans();
			sk.setSeed(rand.nextInt());
			sk.setNumClusters(numClusters);
			sk.setDisplayStdDevs(true);
			sk.buildClusterer(noClassSet);
			if (sk.getSquaredError() < bestSqE) {
				bestSqE = sk.getSquaredError();
				clusterer = sk;
			}
		}

		double[] clusterWeights = new double[numClusters];
		for (int i = 0; i < clusterWeights.length; ++i) {
			clusterWeights[i] = 1.0;
		}

		// The idea was to sample the instances starting from centroids
		// Enumeration centroidEmum =
		// kMeans.getClusterCentroids().enumerateInstances();
		// while(centroidEmum.hasMoreElements()){
		// Instance centroid = (Instance) centroidEmum.nextElement();
		// // cannot get the closest neighbour
		// }

		// // Should provice better clusters (right?), but too slow
		// EM clusterer = new EM();
		// clusterer.setSeed(rand.nextInt());
		// clusterer.buildClusterer(noClassSet);
		// int numClusters = clusterer
		// .getNumClusters();
		// double[] clusterWeights = clusterer.clusterPriors();

		Frequency[] clusterLabelFreq = new Frequency[numClusters];
		for (int i = 0; i < clusterLabelFreq.length; ++i) {
			clusterLabelFreq[i] = new Frequency();
		}

		TreeMap<Double, double[]> instPredictionMap = new TreeMap<Double, double[]>();
		TreeMap<Double, Integer> instInitialLabelMap = new TreeMap<Double, Integer>();
		TreeMap<Double, Integer> instClusterMap = new TreeMap<Double, Integer>();

//		double[][] logDensityPerLabelForClusterParameters = new double[numClusters][Config.LABELS_SINGLES.length];
		// Enumeration instEnum = testSet.enumerateInstances();
		// while (instEnum.hasMoreElements()) {
		for (int i = 0; i < testSet.numInstances(); ++i) {
			Instance testInst = testSet.instance(i);

			double[] labelDistribution = new double[Config.LABELS_SINGLES.length];
			for (int l = 0; l < labelDistribution.length; ++l) {
				labelDistribution[l] = Double.NaN;
			}

			int prevLabelStartIx = testInst.numAttributes()
					- labelDistribution.length - 1;
			for (int l = 0; l < labelDistribution.length; ++l) {
				testInst.setValue(prevLabelStartIx + l, labelDistribution[l]);
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

			Instance noClassInst = noClassSet.instance(i);
			int c = clusterer.clusterInstance(noClassInst);
			instClusterMap.put(noClassInst.value(0), c);

			clusterLabelFreq[c].addValue(predictedLabel);
			instPredictionMap.put(noClassInst.value(0), // ID
					labelDistribution);
			instInitialLabelMap.put(noClassInst.value(0), // ID
					// inst.attribute(inst.numAttributes() - 1).value(
					// predictedLabel));
					predictedLabel);

			// for (int l = 0; l < labelDistribution.length; ++l) {
			// if (labelDistribution[l] > 0) {
			// logDensityPerLabelForClusterPriors[c][l] += Math
			// .log(labelDistribution[l]);
			// }
			// }
		}

		// Keep a copy for history
		double[][] labelPriors = new double[numClusters][Config.LABELS_SINGLES.length];
		for (int c = 0; c < labelPriors.length; ++c) {
			for (int l = 0; l < labelPriors[c].length; ++l) {
				labelPriors[c][l] = clusterLabelFreq[c].getPct(l);
			}
		}

		// Maximize expectation of clusters label assignment
		double[][] emWeights = new double[numClusters][];
		double expectation = 0, expectationOld;
		Enumeration instEnum;
		for (int i = 0; i < Config.CLUSTERCALSSIFY_LABEL_ASSG_MAX_ITERS; ++i) {
			StringBuffer report = new StringBuffer();
			report.append("=============================\n")
					.append("Iteration: " + i).append('\n').append("Cluster");
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				report.append('\t').append("probLabel=" + l);
			}
			report.append('\n');
			for (int c = 0; c < numClusters; ++c) {
				report.append("Cluster" + c);
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					report.append('\t').append(
							Double.toString(clusterLabelFreq[c].getPct(l)));
				}
				report.append('\n');

			}
			System.out.println(report.toString());

			// Start by calculate the log likelihood for each cluster
			double[][] logProbPerLabelForCluster = new double[numClusters][Config.LABELS_SINGLES.length];

			instEnum = testSet.enumerateInstances();
			while (instEnum.hasMoreElements()) {
				Instance inst = (Instance) instEnum.nextElement();
				int c = instClusterMap.get(inst.value(0));
				double[] instLabelDistrib = instPredictionMap
						.get(inst.value(0));
				for (int l = 0; l < instLabelDistrib.length; ++l) {
					if (instLabelDistrib[l] > 0) {
						logProbPerLabelForCluster[c][l] += Math
								.log(instLabelDistrib[l]);
					}
				}
			}
			// double[][] logDensityPerLabelForCluster = new
			// double[numClusters][Config.LABELS_SINGLES.length];
			//
			// Estimate the labels of the clusters (E in the EM)
			double logLkhood = 0.0;
			double sumOfWeights = 0.0;
			for (int c = 0; c < numClusters; ++c) {

				// logDensityPerLabelForCluster[c] = Arrays.copyOf(
				// logDensityPerLabelForClusterPriors[c],
				// Config.LABELS_SINGLES.length);

				// For this cluster, what is the joint probability
				double[] logJointProbForCluster = logProbPerLabelForCluster[c];
				//
				int maxLIx = 0;
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					double prior = clusterLabelFreq[c].getPct(l);
					if (prior > 0) {
						logJointProbForCluster[l] += Math.log(prior);

						if (logJointProbForCluster[l] > logJointProbForCluster[maxLIx]) {
							maxLIx = l;
						}
						// Not needed.. since the index itself will not be used,
						// just the max
						// else if (logJointProbForCluster[l] ==
						// logJointProbForCluster[maxLIx]) {
						// // randomly break tie
						// if(rand.nextBoolean()){
						// maxLIx = l;
						// }
						// }
					}
				}

				double sum = 0.0;
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					sum += Math.exp(logJointProbForCluster[l]
							- logJointProbForCluster[maxLIx]);
				}

				double logProbForCluster = logJointProbForCluster[maxLIx];
				if (sum > 0) {
					logProbForCluster += Math.log(sum);
				}
				// Add

				logLkhood += clusterWeights[c] * logProbForCluster;
				sumOfWeights += clusterWeights[c];

				// Update weights
				emWeights[c] = new double[Config.LABELS_SINGLES.length];
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					emWeights[c][l] = Math.exp(logJointProbForCluster[l]
							- logJointProbForCluster[maxLIx]);
				}
			}

			expectationOld = expectation;
			expectation = logLkhood / sumOfWeights;
			if (i > 0) {
				if ((expectation - expectationOld) < 1e-6) {
					break;
				}
			}

			// Restimate the label probabilities (M step in EM)
			clusterLabelFreq = new Frequency[numClusters];
			for (int c = 0; c < clusterLabelFreq.length; ++c) {
				clusterLabelFreq[c] = new Frequency();
			}

			instEnum = testSet.enumerateInstances();
			while (instEnum.hasMoreElements()) {
				Instance inst = (Instance) instEnum.nextElement();
				int c = instClusterMap.get(inst.value(0));
				double[] instLabelDistrib = instPredictionMap
						.get(inst.value(0));
				double[] modifiedLabelDistrib = new double[instLabelDistrib.length];

				int predictedLabel = -1;
				double predictedLabelProb = Double.NEGATIVE_INFINITY;
				for (int l = 0; l < instLabelDistrib.length; ++l) {
					
					// The restimation is here!
					modifiedLabelDistrib[l] = clusterWeights[c]
							* emWeights[c][l] * instLabelDistrib[l];
					
					
					if (modifiedLabelDistrib[l] > predictedLabelProb) {
						predictedLabelProb = modifiedLabelDistrib[l];
						predictedLabel = l;
					} else if (modifiedLabelDistrib[l] == predictedLabelProb) {
						if (rand.nextBoolean()) {
							predictedLabelProb = modifiedLabelDistrib[l];
							predictedLabel = l;
						}
					}
				}

				// And it carries over to the next iteration here
				instPredictionMap.put(inst.value(0), modifiedLabelDistrib);
				clusterLabelFreq[c].addValue(predictedLabel);
			}
		}

		long[] clusterLabels = new long[numClusters];
		for (int c = 0; c < clusterLabels.length; ++c) {
			Iterator<Comparable<?>> valsIter = clusterLabelFreq[c]
					.valuesIterator();
			double lPct = Double.NEGATIVE_INFINITY;
			while (valsIter.hasNext()) {
				Long l = (Long) valsIter.next();
				if (clusterLabelFreq[c].getPct(l) > lPct || (clusterLabelFreq[c].getPct(l) == lPct && rand.nextBoolean())) {
					clusterLabels[c] = l;
					lPct = clusterLabelFreq[c].getPct(l);
				} 
			}
		}
		TreeMap<Double, Integer> instTrueLabelMap = null;
		if (validate) {
			instTrueLabelMap = new TreeMap<Double, Integer>();
		}
		TreeMap<Double, Integer> instFinalLabelMap = new TreeMap<Double, Integer>();
		instEnum = testSet.enumerateInstances();
		while (instEnum.hasMoreElements()) {
			Instance inst = (Instance) instEnum.nextElement();
			int c = instClusterMap.get(inst.value(0));

			instFinalLabelMap.put(inst.value(0),
			// inst.attribute(inst.numAttributes() - 1).value(
					(int) clusterLabels[c]);

			if (validate) {
				instTrueLabelMap.put(inst.value(0),
						(int) Math.round(inst.value(inst.numAttributes() - 1)));
			}
		}

		UserPrediction result = new UserPrediction();
		result.numClusters = numClusters;
		result.instFinalLabelMap = instFinalLabelMap;
		result.instInitialLabelMap = instInitialLabelMap;
		result.instClusterMap = instClusterMap;
		result.clusterLabels = clusterLabels;
		result.clusterLabelFreq = clusterLabelFreq;
		result.clusterLabelPriors = labelPriors;
		result.instTrueLabelMap = instTrueLabelMap;

		return result;
	}

	public class UserPrediction {
		public TreeMap<Double, Integer> instTrueLabelMap;
		public int numClusters;
		public double[][] clusterLabelPriors;
		public Frequency[] clusterLabelFreq;
		public long[] clusterLabels;
		public TreeMap<Double, Integer> instClusterMap;
		public TreeMap<Double, Integer> instInitialLabelMap;
		public TreeMap<Double, Integer> instFinalLabelMap;

		@Override
		public String toString() {

			StringBuffer result = new StringBuffer();
			result.append("Cluster Labels:\n").append("=================\n")
					.append("Cluster");
			for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
				result.append('\t').append("priorLabel=" + l);
				result.append('\t').append("posteriorLabel=" + l);
			}
			result.append('\n');
			for (int c = 0; c < numClusters; ++c) {
				result.append("Cluster" + c + "=" + clusterLabels[c]);
				for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
					result.append('\t')
							.append(Double.toString(clusterLabelPriors[c][l]))
							.append('\t')
							.append(Double.toString(clusterLabelFreq[c]
									.getPct(l)));
				}
				result.append('\n');
			}
			result.append('\n');

			result.append("Instances Clusters and Labels:\n")
					.append("====================================\n")
					.append("InstID\tCluster\tinitialLabel\tfinalLabel");
			if (validate) {
				result.append("\ttrueLabel");
			}
			result.append('\n');

			int countChangedLabels = 0;
			for (Double instID : instClusterMap.keySet()) {
				result.append(instID).append('\t')
						.append(instClusterMap.get(instID)).append('\t')
						.append(instInitialLabelMap.get(instID)).append('\t')
						.append(instFinalLabelMap.get(instID));
				if (validate) {
					result.append('\t').append(instTrueLabelMap.get(instID));
				}
				result.append('\n');

				if (instInitialLabelMap.get(instID) != instFinalLabelMap
						.get(instID)) {
					++countChangedLabels;
				}
			}
			result.append('\n');

			result.append("Instances That Changed Label: ")
					.append(countChangedLabels)
					.append(" / ")
					.append(instClusterMap.keySet().size())
					.append(" = ")
					.append(countChangedLabels * 1.0
							/ instClusterMap.keySet().size());

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

		baseClassifier = new J48();
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

	public static void main(String[] args) throws Exception {
		ExecutorService diffClusterNumExecutor = Executors.newFixedThreadPool(Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MAX - Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MIN + 1);
		// TODO: iterate over folds here, and train classifier once for each fold
		Future<Void> lastFuture = null;
		for (int c = Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MIN; c <= Config.CLUSTERCLASSIFY_NUM_CLUSTERS_MAX; ++c) {
			ClusterClassifyEM app = new ClusterClassifyEM(c);
			lastFuture = diffClusterNumExecutor.submit(app);
		}
		
		// I bet their implementation of waiting is smarter
		// that the busy wait below (but don't remove it)
		lastFuture.get();
		
		diffClusterNumExecutor.shutdown();
		
		while(!diffClusterNumExecutor.isTerminated()){
			Thread.sleep(5000);
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
}
