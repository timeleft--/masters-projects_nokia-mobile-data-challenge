package uwaterloo.mdc.weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.util.NotifyStream;
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.AttributeTransformer;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.ConsistencySubsetEval;
import weka.attributeSelection.GainRatioAttributeEval;
import weka.attributeSelection.GreedyStepwise;
import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.attributeSelection.ReliefFAttributeEval;
import weka.attributeSelection.SubsetEvaluator;
import weka.attributeSelection.WrapperSubsetEval;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.lazy.KStar;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.meta.ClassificationViaClustering;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.clusterers.SimpleKMeans;
import weka.core.DistanceFunction;
import weka.core.EuclideanDistance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SelectedTag;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;

public class ClassifyAndFeatSelect implements Callable<Void> {
	// private static boolean validate = true;
	private double superiorityRatio = 1.0;

	// Commented out are Dimensionality reductions that failed
	// Class<? extends ASEvaluation>[] cannot create!
	@SuppressWarnings("rawtypes")
	public static Class[] attrSelectorsForMain = {

			// ChiSquaredAttributeEval.class,
			// causes nonunique name exception LatentSemanticAnalysis.class,
			// FilteredAttributeEval.class, FilteredSubsetEval.class,
			// InfoGainAttributeEval.class,
			// SVMAttributeEval.class,
			// SymmetricalUncertAttributeEval.class,
			GainRatioAttributeEval.class, ReliefFAttributeEval.class,
			CfsSubsetEval.class, PrincipalComponents.class,
			ConsistencySubsetEval.class,

			WrapperSubsetEval.class,

	};
	// // // Attribute Transformers
	// // public static Class[] attrTransEvaluationClazzes = {
	// // // PrincipalComponents and
	// // LatentSemanticAnalysis.class, };
	public static final String ALL_FEATS = "all-features";
	private static final String INTERNAL_DIR_PFX = "internal-node_";
	private static final Integer NUMBER_OF_ALGORITHMS = 1;

	private class FoldCallable implements
			Callable<KeyValuePair<String, HashMap<String, Double>>> {

		final Pattern positiveSplit = Pattern.compile("\\+");
		final Pattern minusSplit = Pattern.compile("\\-");

		final Classifier baseClassifier;

		final int v;
		Random rand = new Random(System.currentTimeMillis());

		private final File positiveClassDir;
		private String positiveClass;
		private String[] pSplits;
		private String[] mSplits;
		private boolean classifyingBinary;
		// private double superiorityRatio = 1.0 + n; // / 10.0;
		final Class attSelClazz;
		private final ASSearch search;
		// private Instances trainingSet;
		private final ASEvaluation eval;

		public FoldCallable(File positiveClassDir, int fold, Class attSelClazz,
				ASSearch search, ASEvaluation eval) throws Exception {
			this.positiveClassDir = positiveClassDir;
			this.attSelClazz = attSelClazz;
			this.search = search;
			this.eval = eval;
			// if (validate) {
			positiveClass = positiveClassDir.getName();
			pSplits = positiveSplit.split(positiveClass);
			mSplits = minusSplit.split(positiveClass);
			// } else {
			// positiveClass = Config.LABELS_MULTICLASS_NAME;
			// }
			this.v = fold;
			baseClassifier = baseClassifierClazz.getConstructor().newInstance();
			if (baseClassifier instanceof LibSVM) {
				// baseClassifier.setDebug(false);
				((LibSVM) baseClassifier).setKernelType(new SelectedTag(
						LibSVM.KERNELTYPE_RBF, LibSVM.TAGS_KERNELTYPE));
				// WARNING: using -h 0 may be faster
				((LibSVM) baseClassifier).setShrinking(false);
			} else if (baseClassifier instanceof J48) {
				// ((J48) baseClassifier).setConfidenceFactor(0.TODO How to
				// prevent over fitting??)
			} else if (baseClassifier instanceof ClassificationViaClustering) {
				// EM em = new EM();
				// em.setSeed(new Random(System.currentTimeMillis()).nextInt());
				// XMeans xm = new XMeans();
				// xm.setSeed(new Random(System.currentTimeMillis()).nextInt());
				// xm.setMinNumClusters(4);
				// xm.setMaxNumClusters(11);
				SimpleKMeans km = new SimpleKMeans();
				km.setSeed(new Random(System.currentTimeMillis()).nextInt());
				km.setNumClusters(11);
				((ClassificationViaClustering) baseClassifier).setClusterer(km);
			} else if (baseClassifier instanceof RandomForest) {
				// ((RandomForest) baseClassifier).setMaxDepth(13);
			}

			classifyingBinary = classifyBinaryHierarchy
					&& !Config.LABELS_MULTICLASS_NAME.equals(positiveClass);
		}

		@Override
		public KeyValuePair<String, HashMap<String, Double>> call() {
			HashMap<String, Double> accuracyMap = new HashMap<String, Double>();

			try {
				Frequency[] foldConfusionMatrix;
				foldConfusionMatrix = new Frequency[Config.LABELS_SINGLES.length];
				for (int i = 0; i < foldConfusionMatrix.length; ++i) {
					foldConfusionMatrix[i] = new Frequency();
				}

				Frequency[] foldFeactSelectCM;
				foldFeactSelectCM = new Frequency[Config.LABELS_SINGLES.length];
				for (int i = 0; i < foldFeactSelectCM.length; ++i) {
					foldFeactSelectCM[i] = new Frequency();
				}

				int foldStart = v * 1; // FIXME: Config.VALIDATION_FOLD_WIDTH;

				// trainingSet = instsKV.getKey();

				try {
					// if (baseClassifier instanceof UpdateableClassifier) {
					// // already trained
					// } else {
					// And this is not used anymore now that we don't do
					// baseline baseClassifier.buildClassifier(trainingSet);
					// THE ANNIHILATOR OF SUCCESS: trainingSet = null;
					// }

					System.out.println(baseClassifierClazz.getSimpleName()
							+ " - " + (System.currentTimeMillis() - startTime)
							+ " (fold " + v
							+ "): Finished training for positive class: "
							+ positiveClass);

					// ////////////////////*********************/////////////////////////
					Properties[] validationActualCs = new Properties[1];// validationSet.length];//

					for (int u = 0; u < 1 /* validationSet.length */; ++u) {
						// This should never happen
						// if (validationSet == null ||
						// validationSet.numInstances()
						// == 0) {
						// System.err.println("No validation data for fold: " +
						// v
						// + " - class: " + positiveClass);
						// return new KeyValuePair<String, HashMap<String,
						// Double>>(
						// positiveClass, accuracyMap);
						// }

						validationActualCs[u] = new Properties();
						if (classifyingBinary) {
							validationActualCs[u]
									.load(Channels.newReader(
											FileUtils
													.openInputStream(
															FileUtils
																	.getFile(
																			positiveClassDir,
																			// validationSet[u].relationName()
																			// +
																			"_actual-labels.properties"))
													.getChannel(),
											Config.OUT_CHARSET));
						}
					}

					// No need to calculate baseline anymore
					// Writer classificationsWr = Channels
					// .newWriter(
					// FileUtils
					// .openOutputStream(
					// FileUtils
					// .getFile(
					// outputPath,
					// baseClassifier
					// .getClass()
					// .getName(),
					// "v"
					// + v
					// + "_u"
					// + u + "_n" + n
					// + "_"
					// + positiveClass
					// + "_classifications.txt"))
					// .getChannel(),
					// Config.OUT_CHARSET);
					//
					// try {
					// classificationsWr
					// .append("place-id\tclass0\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\ttrueClass\n");
					//
					// // TODONOT: user 113
					//
					// int trueClassificationsCount = 0;
					// int trueInternalDirectionCount = 0;
					// int totalClassificationsCount = (classifyingBinary ? 0
					// : validationSet[u].numInstances());
					// int totalInternalDirections = 0;
					// // Well there aren't any dumb
					// // // I don't know how some data still have missing
					// // // clas
					// // validationSet.deleteWithMissingClass();
					//
					// int prevLabelStart = validationSet[u]
					// .numAttributes()
					// - Config.LABELS_SINGLES.length - 1;
					// double prevInstLabel = Double.NaN;
					// // (Config.LOAD_REPLACE_MISSING_VALUES ?
					// // Config.LOAD_MISSING_VALUE_REPLA
					// // : Double.NaN);
					//
					// Properties instIdPlaceId = new Properties();
					// Reader idsRd = Channels
					// .newReader(
					// FileUtils
					// .openInputStream(
					// FileUtils
					// .getFile(
					// inPath,
					// validationSet[u]
					// .relationName()
					// + "_instid-placeid_map.properties"))
					// .getChannel(),
					// Config.OUT_CHARSET);
					// instIdPlaceId.load(idsRd);
					// idsRd.close();
					//
					// for (int i = 0; i < validationSet[u].numInstances(); ++i)
					// {
					// Instance vInst = validationSet[u].instance(i);
					//
					// if (ignoreInstsWithMissingClass
					// && vInst.classIsMissing()) {
					// continue;
					// }
					//
					// Instance classMissing = (Instance) vInst.copy();
					// classMissing.setDataset(vInst.dataset());
					// if (!(baseClassifier instanceof
					// ClassificationViaClustering)) {
					// classMissing.setClassMissing();
					// }
					// if (!classifyingBinary) {
					// for (int p = 0; p < Config.LABELS_SINGLES.length; ++p) {
					// classMissing
					// .setValue(prevLabelStart + p,
					// p == prevInstLabel ? 1
					// : 0);
					// }
					// }
					// double[] vClassDist = baseClassifier
					// .distributionForInstance(classMissing);
					// // FIXME: use the ID attribute
					// String placeId = instIdPlaceId
					// .getProperty(Double
					// .toString((i + 1) * 1.0));
					// classificationsWr.append(placeId);
					// // vInst.dataset()C.relationName() + "[" + (i +
					// // "]");
					// // double vClassMaxProb =
					// // Double.NEGATIVE_INFINITY;
					// // double vClass = -1;
					// // for (int j = 0; j < vClassDist.length; ++j) {
					// // classificationsWr.append("\t"
					// // + vClassDist[j]);
					// // if (vClassDist[j] > vClassMaxProb) {
					// // vClassMaxProb = vClassDist[j];
					// // vClass = j;
					// // }
					// // }
					// for (int j = 0; j < vClassDist.length; ++j) {
					// classificationsWr.append("\t"
					// + vClassDist[j]);
					// }
					// double vClass = getClassFromDistrib(vClassDist,
					// classMissing);
					// classificationsWr
					// .append('\t')
					// .append(Double.toString(vInst
					// .classValue())).append('\n');
					// synchronized (placeFreqMap) {
					// Map<String, Frequency> placeFrequencyMap = placeFreqMap
					// .get(ALL_FEATS);
					// if (placeFrequencyMap == null) {
					// placeFrequencyMap = new TreeMap<String, Frequency>();
					// placeFreqMap.put(ALL_FEATS,
					// placeFrequencyMap);
					// }
					// Frequency placeFreq = placeFrequencyMap
					// .get(placeId);
					// if (placeFreq == null) {
					// placeFreq = new Frequency();
					// placeFrequencyMap.put(placeId,
					// placeFreq);
					// }
					// placeFreq.addValue(Long.toString(Math
					// .round(vClass)));
					// }
					// prevInstLabel = vClass;
					// // The class "Value" is actually its
					// // index!!!!!!
					// if (vClass == vInst.classValue()) {
					// // Correct.. but what
					// if (classifyingBinary) {
					// if (// +1 going to a leaf
					// (vClass == Config.LABLES_BINARY_POSITIVE_IX &&
					// pSplits.length == 3)
					// // -1 going to a leaf
					// || (vClass == Config.LABLES_BINARY_NEGATIVE_IX &&
					// mSplits.length == 3)) {
					//
					// // // there is only 1 +C+
					// // String actualC =
					// // validationActualCs
					// // .getProperty(Long.toString(Math
					// // .round(vInst.value(0))));
					// // if (pSplits[1].equals(actualC)) {
					// ++trueClassificationsCount;
					//
					// } else if (// +1 going to a non-leaf
					// (vClass == Config.LABLES_BINARY_POSITIVE_IX &&
					// pSplits.length > 3)
					// // -1 going to a non-leaf
					// || (vClass == Config.LABLES_BINARY_NEGATIVE_IX &&
					// mSplits.length > 3)) {
					//
					// // No meaning for accuracy at non
					// // leaves
					// // ++trueClassificationsCount;
					// ++trueInternalDirectionCount;
					// }
					// } else {
					// ++trueClassificationsCount;
					// }
					// }
					// if (classifyingBinary) {
					// // Count number that should go to a class
					// // leaf or get directed to a subtree
					// if (vInst.classValue() == 0) {
					// // +1
					// if (pSplits.length == 3) {
					// ++totalClassificationsCount;
					// } else {
					// ++totalInternalDirections;
					// }
					// } else {
					// // -1
					// if (mSplits.length == 3) {
					// ++totalClassificationsCount;
					// } else {
					// ++totalInternalDirections;
					// }
					// }
					// }
					//
					// long trueLabelCfMIx;
					// if (classifyingBinary) {
					// // Get the true label from the properties
					// // file,
					// // Using the value of the ID attribute as
					// // key
					// // ID is at index 0
					// try {
					// trueLabelCfMIx = Long
					// .parseLong(validationActualCs[u].getProperty(Double
					// .toString(
					// // Long.toString(Math.round(
					// vInst.value(0))));
					// } catch (NumberFormatException ignored) {
					// trueLabelCfMIx = 0;
					// System.err
					// .println(baseClassifierClazz
					// .getSimpleName()
					// + " - "
					// + (System
					// .currentTimeMillis() - startTime)
					// + " (fold "
					// + v
					// + " - "
					// + u + "_n" + n
					// + "): Cannot get true label for instance ID "
					// + vInst.value(0)
					// + " in positive class "
					// + positiveClass);
					// }
					// } else {
					// trueLabelCfMIx = Math.round(vInst
					// .classValue());
					// }
					// // Will not happen at all.. remember that
					// // .classvalue returns the index in the nominal
					// // if(Config.CLASSIFY_USING_BIANRY_ENSEMBLE){
					// // if(trueLabelCfMIx == -1){
					// // trueLabelCfMIx = 0;
					// // }
					// // }
					//
					// long bestLabelInt = Math.round(vClass);
					// // foldConfusionMatrix[(int) trueLabelCfMIx]
					// // .addValue(bestLabelInt);
					// foldConfusionMatrix[(int) trueLabelCfMIx]
					// .addValue((classifyingBinary ? vClass == (vInst
					// .classValue()) : bestLabelInt));
					// synchronized (totalConfusionMatrix) {
					// totalConfusionMatrix.get(positiveClass)[(int)
					// trueLabelCfMIx]
					// // .addValue(bestLabelInt);
					// .addValue((classifyingBinary ? vClass == (vInst
					// .classValue())
					// : bestLabelInt));
					// }
					// }
					// if (totalClassificationsCount > 0) {
					// // FIXME: Support more than just LOO-CV by
					// // adding u
					// // to ALL_FEATS
					// // and other map keys... and handling it in
					// // gather
					// // and print. Duh!
					// accuracyMap.put(ALL_FEATS,
					// trueClassificationsCount * 1.0
					// / totalClassificationsCount);
					//
					// synchronized (cvClassificationAccuracyWr) {
					//
					// cvClassificationAccuracyWr
					// .get(positiveClass)
					// .append(Integer.toString(v))
					// .append('\t')
					// .append(Integer
					// .toString(trueClassificationsCount))
					// .append('\t')
					// .append(Integer
					// .toString(totalClassificationsCount))
					// .append('\t')
					// .append(Double.toString(accuracyMap
					// .get(ALL_FEATS)))
					// .append('\n');
					// }
					// }
					// if (totalInternalDirections > 0) {
					// accuracyMap.put(INTERNAL_DIR_PFX + ALL_FEATS,
					// trueInternalDirectionCount * 1.0
					// / totalInternalDirections);
					//
					// synchronized (cvClassificationAccuracyWr) {
					//
					// cvClassificationAccuracyWr
					// .get(positiveClass)
					// .append(INTERNAL_DIR_PFX
					// + Integer.toString(v))
					// .append('\t')
					// .append(Integer
					// .toString(trueInternalDirectionCount))
					// .append('\t')
					// .append(Integer
					// .toString(totalInternalDirections))
					// .append('\t')
					// .append(Double.toString(accuracyMap
					// .get(INTERNAL_DIR_PFX
					// + ALL_FEATS)))
					// .append('\n');
					// }
					// }
					// } finally {
					// classificationsWr.flush();
					// classificationsWr.close();
					// }
					// Writer foldConfusionWr = Channels
					// .newWriter(
					// FileUtils
					// .openOutputStream(
					// FileUtils
					// .getFile(
					// outputPath,
					// baseClassifier
					// .getClass()
					// .getName(),
					// "v"
					// + v
					// + "_u"
					// + u + "_n" + n
					// + "_"
					// + positiveClass
					// + "_confusion-matrix.txt"))
					// .getChannel(),
					// Config.OUT_CHARSET);
					// try {
					// writeConfusionMatrix(foldConfusionWr,
					// foldConfusionMatrix, classifyingBinary);
					// } finally {
					// foldConfusionWr.flush();
					// foldConfusionWr.close();
					// }
					// }
					// System.out.println(baseClassifierClazz.getSimpleName()
					// + " - " + (System.currentTimeMillis() - startTime)
					// + " (fold " + v
					// + "): Finished validation for  class: "
					// + positiveClass);
					//
					// // //////////////////////////////////////
					if (featSelect) {
						// for (@SuppressWarnings("rawtypes")
						// Class attrSelectEvalClazz :
						// attrSelectEvaluationClazzes) {
						//
						// @SuppressWarnings("unchecked")
						// ASEvaluation eval = (ASEvaluation)
						// attrSelectEvalClazz
						// .getConstructor().newInstance();
						//
						// if (eval instanceof GainRatioAttributeEval) {
						// ((GainRatioAttributeEval) eval)
						// .setMissingMerge(false);
						// }
						//
						// ASSearch search;
						// if (eval instanceof SubsetEvaluator) {
						// search = new GreedyStepwise();
						// // ((GreedyStepwise)
						// // search).setSearchBackwards(true);
						// // ((GreedyStepwise)
						// // search).setNumToSelect(this.baseClassifier.)
						// ((GreedyStepwise) search)
						// .setGenerateRanking(true);
						// } else {
						// search = new Ranker();
						// }

						// if (eval instanceof SVMAttributeEval) {
						//
						// Add add = new Add();
						// add.setAttributeIndex("last");
						// add.setAttributeName("binary-label");
						// add.setNominalLabels("+1,-1");
						// add.setInputFormat(validationSet);
						//
						// for (String positiveClass :
						// Config.LABEL_HIERARCHY) { // classes
						//
						// Instances copyValidation = new Instances(
						// validationSet);
						//
						// // Should have done this earlier
						// // actually
						// copyValidation.deleteWithMissingClass();
						//
						// copyValidation = Filter.useFilter(
						// copyValidation, add);
						//
						// @SuppressWarnings("rawtypes")
						// Enumeration instEnum = copyValidation
						// .enumerateInstances();
						// int positiveExamples = 0;
						// while (instEnum.hasMoreElements()) {
						// Instance copyInst = (Instance) instEnum
						// .nextElement();
						//
						// String cls = Long.toString(Math
						// .round(copyInst
						// .classValue()) ;
						//
						// String binaryLabel = null;
						// if (positiveClass.contains("+"
						// + cls + "+")) {
						// binaryLabel = "+1";
						// ++positiveExamples;
						//
						// } else if (positiveClass
						// .contains("-" + cls + "-")) {
						// binaryLabel = "-1";
						// }
						//
						// if (binaryLabel != null) {
						// copyInst.setValue(copyInst
						// .numAttributes() - 1,
						// binaryLabel);
						// } else {
						// copyInst.setMissing(copyInst
						// .numAttributes() - 1);
						// }
						//
						// copyInst.setDataset(copyValidation);
						//
						// }
						//
						// Remove rem = new Remove();
						// // The index range starts from 1 when it
						// // is
						// // text
						// rem.setAttributeIndices(Integer
						// .toString(copyValidation
						// .numAttributes() - 1));
						// rem.setInputFormat(copyValidation);
						// copyValidation = Filter.useFilter(
						// copyValidation, rem);
						//
						// copyValidation
						// .setClassIndex(copyValidation
						// .numAttributes() - 1);
						// copyValidation.deleteWithMissingClass();
						//
						// copyValidation
						// .setRelationName(validationSet
						// .getRevision()
						// + positiveClass);
						//
						// if (positiveExamples > 0) {
						// HashMap<String, Double> tempAccuracy = new
						// HashMap<String, Double>();
						// Frequency[] tempCM;
						// tempCM = new
						// Frequency[Config.LABEL_HIERARCHY.length];
						// for (int c = 0; c < tempCM.length; ++c) {
						// tempCM[c] = new Frequency();
						// }
						// featSel(copyValidation, tempCM,
						// eval, search, tempAccuracy,
						// "v" + v + "_"
						// + positiveClass);
						// for (String accuKeys : tempAccuracy
						// .keySet()) {
						// accuracyMap
						// .put(accuKeys
						// + positiveClass,
						// tempAccuracy
						// .get(accuKeys));
						// }
						// // The confusion matrix needs some
						// // tweeking,
						// // lest it throws exceptions (Null
						// // or o
						// // bound)
						// // Writer tempCmWr = Channels
						// // .newWriter(
						// // FileUtils
						// // .openOutputStream(
						// // FileUtils
						// // .getFile(
						// // outputPath,
						// // baseClassifier
						// // .getClass()
						// // .getName(),
						// // "v"
						// // + v
						// // + "svm"
						// // + positiveClass
						// // + "_confusion-matrix.txt"))
						// // .getChannel(),
						// // Config.OUT_CHARSET);
						// // try {
						// // writeConfusionMatrix(tempCmWr,
						// // tempCM);
						// // } finally {
						// // tempCmWr.flush();
						// // tempCmWr.close();
						// // }
						// }
						// }
						// } else {

						for (int u = 0; u < 1 /* validationSet.length */; ++u) {
							featSel(foldFeactSelectCM, eval, search,
									accuracyMap, "v" + v + "_u" + u + "_n"
											+ superiorityRatio,
									validationActualCs[u]);
						}

						// }

						// // //////////////////////////////////////
						// // //////////////////////////////////////
						//
						// for (@SuppressWarnings("rawtypes")
						// Class attrTransEvalClazz :
						// attrTransEvaluationClazzes) {
						//
						// @SuppressWarnings("unchecked")
						// ASEvaluation eval = (ASEvaluation)
						// attrTransEvalClazz
						// .getConstructor().newInstance();
						//
						// ASSearch search;
						// search = new Ranker();
						//
						// featSel(validationSet, foldFeactSelectCM, eval,
						// search,
						// accuracyMap);
						//
						// }
						// //////////////////////////////////////
					}
				} finally {
					// if (validate) {
					foldFinished(positiveClassDir.getName(), foldStart);

				}
			} catch (Exception ignored) {
				ignored.printStackTrace(System.err);
			}

			return new KeyValuePair<String, HashMap<String, Double>>(
					positiveClass, accuracyMap);
		}

		Frequency nearTies;
		SummaryStatistics tieSize;
		Frequency prevLabelCorrect;
		Frequency randLabelCorrect;
		LinkedList<Instance> withinDayInsts;
		static final int maxVisitsInADay = 1;// (6 * 24); // 10;// ;
		DistanceFunction distanceMeasure;
		private double[][] instDistribs;

		private double getClassFromDistrib(double[] vClassDist, Instance inst,
				double trueClass) {
			double vClassMaxProb = 1E-9;// vClassDist[0];
			LinkedList<Integer> result = new LinkedList<Integer>();
			double res = Double.NaN;
			for (int j = 0; j < vClassDist.length; ++j) {
				double probDiv = vClassDist[j] / vClassMaxProb;
				if (probDiv >= 1 / superiorityRatio) {
					if (probDiv > superiorityRatio) {
						// pure
						result.clear();
					}
					result.add(j);
					if (probDiv > 1) {
						vClassMaxProb = vClassDist[j];
					}
				}

			}

			if (result.size() == 1) {
				res = result.getFirst().doubleValue();
			} else if (result.size() > 1) {
				tieSize.addValue(result.size());
				// Integer prevLabel = null;
				// int prevAttrsIx = inst.numAttributes()
				// - Config.LABELS_SINGLES.length - 1;
				// for (int a = 0; a < Config.LABELS_SINGLES.length; ++a) {
				// if (inst.value(prevAttrsIx + a) > 0) {
				// prevLabel = a;
				// break;
				// }
				// }
				Iterator<Instance> instsIter = withinDayInsts.iterator();
				int numStepsBack = 0;
				int numStepsBackAtSelection = -1;

				Instance closestInst = null;
				double minD = Double.MAX_VALUE;
				while (instsIter.hasNext()) {
					Instance prevInst = instsIter.next();
					double d = distanceMeasure.distance(inst, prevInst);
					// penlaty * (1+(numStepsBack / maxVisitsInADay))
					if (d < minD) {
						// prevLabel = (int) Math.round(prevInst.classValue());
						closestInst = prevInst;
						minD = d;
						numStepsBackAtSelection = numStepsBack;
					}
					++numStepsBack;
				}

				// if (prevLabel != null
				// // attempt anything when the prev is just 0
				// && prevLabel != 0) {
				// // People tend to stay at home or at work
				// &&(prevLabel == 1 || prevLabel == 2)) {

				if (closestInst != null) {
					Integer prevLabel = (int) Math.round(closestInst
							.classValue());
					if (result.contains(prevLabel)
					// Use the past only if it is better
							&& (closestInst.weight() > vClassMaxProb)) {
						// Use the nearest neighbour if it is already a probable
						System.out.println("Using the "
								+ (numStepsBackAtSelection + 1)
								+ " previous label " + prevLabel
								+ " to break near tie between " + result
								+ " from distrib "
								+ Arrays.toString(vClassDist));
						nearTies.addValue('P');

						res = prevLabel.doubleValue();

						// if(validate){
						prevLabelCorrect.addValue(res == trueClass);
						// }
					}
				}
				// else {
				if (Double.isNaN(res)) {
					System.out.println("Randomly breaking a near tie between "
							+ result + " from distrib "
							+ Arrays.toString(vClassDist));
					nearTies.addValue('R');

					res = result.get(rand.nextInt(result.size())).doubleValue();

					// if(validate){
					randLabelCorrect.addValue(res == trueClass);
					// }
				}
			}

			inst.setClassValue(res);
			inst.setWeight(vClassMaxProb);
			withinDayInsts.addFirst(inst);
			while (withinDayInsts.size() > maxVisitsInADay) {
				withinDayInsts.removeLast();
			}
			return res;
		}

		void featSel(Frequency[] foldFeactSelectCM, ASEvaluation eval,
				ASSearch search, HashMap<String, Double> accuracyMap,
				String filenamePfx, Properties validationActualCs)
				throws Exception {

			KeyValuePair<Instances[], Instances[]> insts = loadFold(
					positiveClassDir, v);
			Instances trainingSet = insts.getKey()[0];
			// Instances[] validationSetArr;
			// if(Config.CLASSIFY_SEPARATE_PREVALENCE){
			// validationSetArr = insts.getValue();
			// } else {
			// validationSetArr = new Instances[4];
			// copyInstancesIntoSets( insts.getValue()[0], validationSetArr);
			// }
			String fileNamePPfxBase = filenamePfx;
			int levels = (Config.CLASSIFY_SEPARATE_PREVALENCE ? 4 : 1);
			instDistribs = new double[trainingSet.numInstances()][Config.LABELS_SINGLES.length];
			for (int l = 0; l < levels; ++l) {
				AttributeSelectedClassifier featSelector = new AttributeSelectedClassifier();
				Classifier classifier;
				ASEvaluation evaluation;
				if ((Config.CLASSIFY_SEPARATE_PREVALENCE) && (l < 2)) {
					evaluation = new PrincipalComponents();
					classifier = new J48(); // DecisionStump();
					// Cannot handle a binary class
					// classifier = new LinearRegression();
					// ((LinearRegression)classifier).setRidge(newRidge)
					// classifier = new NaiveBayes();
					// classifier = new KStar();
					// ((KStar)classifier).se
					// classifier = new ClassificationViaRegression();
					// classifier = new ClassificationViaClustering();
					// ((ClassificationViaClustering)classifier).setClusterer(new
					// XMeans());
					// classifier = new LibSVM();
					// ((LibSVM) classifier).setKernelType(new SelectedTag(
					// LibSVM.KERNELTYPE_LINEAR, LibSVM.TAGS_KERNELTYPE));
					// // WARNING: using -h 0 may be faster
					// ((LibSVM) classifier).setShrinking(false);
				} else {
					classifier = baseClassifier;
					evaluation = eval;
				}
				featSelector.setClassifier(classifier);
				featSelector.setEvaluator(evaluation);
				featSelector.setSearch(search);

				if (Config.CLASSIFY_SEPARATE_PREVALENCE) {

					trainingSet = copyInstancesIntoSets(insts.getKey()[0], l,
							true);

				}

				featSelector.buildClassifier(trainingSet);

				if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
					filenamePfx = fileNamePPfxBase + "_l" + l;
					if (l == 0) {
						pSplits = new String[] { "0", "Aginst", "Rest" };
					} else {
						pSplits = new String[] {};
					}
					if (l < 2) {
						classifyingBinary = true;
					} else {
						classifyingBinary = false;

					}
				}

				Instances validationSet;
				if (!Config.CLASSIFY_SEPARATE_PREVALENCE) {
					validationSet = insts.getValue()[0];
				} else {
					validationSet = copyInstancesIntoSets(insts.getValue()[0],
							l, false); // true
				}
				// = validationSetArr[l];
				// if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
				// Instances copyValidation;
				// if (l < 2) {
				// copyValidation = changeStruct(validationSet);
				// } else {
				// copyValidation = new Instances(validationSet, 0);
				// }
				// Enumeration vEnum = validationSet.enumerateInstances();
				// while (vEnum.hasMoreElements()) {
				// Instance vInst = (Instance) vEnum.nextElement();
				// Instance[] copies = spreadInstOnSets(vInst,copyValidation);
				// if (copies[l] != null) {
				// copyValidation.add(copies[l]);
				// }
				// }
				// validationSet = copyValidation;
				// }

				nearTies = new Frequency();
				tieSize = new SummaryStatistics();
				prevLabelCorrect = new Frequency();
				randLabelCorrect = new Frequency();
				withinDayInsts = new LinkedList<Instance>();
				distanceMeasure = new EuclideanDistance(validationSet);
				// FIXME: This function wasn't modified for handling more than
				// LOO
				// CV at all.. look into it if you support it
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
																eval.getClass()
																		// attrSelectEvalClazz
																		.getName(),
																filenamePfx
																		+ "_"
																		+ positiveClass
																		+ "_feat-selected-classifications.txt"))
										.getChannel(), Config.OUT_CHARSET);
				try {

					featSelectWr
							.append("placeid\tclass0Prob\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\ttrueLabel\n");
					if (validationSet.numInstances() == 0) {
						featSelectWr.append("Not validation data for fold: "
								+ v);

					} else {
						int featSelectCorrectCount = 0;
						int featSelTotalCount = (classifyingBinary ? 0
								: validationSet.numInstances());
						int totalInternalDirections = 0;
						int trueInternalDirectionCount = 0;
						int prevLabelStart = validationSet.numAttributes()
								- Config.LABELS_SINGLES.length - 1;
						double prevInstLabel = Double.NaN;
						// (Config.LOAD_REPLACE_MISSING_VALUES ?
						// Config.LOAD_MISSING_VALUE_REPLA
						// : Double.NaN);

						Properties instIdPlaceId = new Properties();
						Reader idsRd = Channels
								.newReader(
										FileUtils
												.openInputStream(
														FileUtils
																.getFile(
																		inPath,
																		validationSet
																				.relationName()
																				+ "_instid-placeid_map.properties"))
												.getChannel(),
										Config.OUT_CHARSET);
						instIdPlaceId.load(idsRd);
						idsRd.close();

						BufferedReader instIDVisitTimeRd = new BufferedReader(
								Channels.newReader(
										FileUtils
												.openInputStream(
														FileUtils
																.getFile(
																		inPath,
																		validationSet
																				.relationName()
																				+ "_instid-time_map.properties"))
												.getChannel(),
										Config.OUT_CHARSET));

						Writer instIDVisitTimeWr = Channels
								.newWriter(
										FileUtils
												.openOutputStream(
														FileUtils
																.getFile(
																		outputPath,
																		baseClassifier
																				.getClass()
																				.getName(),
																		eval.getClass() // attrSelectEvalClazz
																				.getName(),
																		filenamePfx
																				+ "_"
																				+ positiveClass
																				+ "_feat-selected_instid-time_prediction.csv"))
												.getChannel(),
										Config.OUT_CHARSET);
						try {
							instIDVisitTimeWr.append(instIDVisitTimeRd
									.readLine()
									+ "\tPredictedLabel\tActualLabel\n");

							for (int i = 0; i < validationSet.numInstances(); ++i) {
								Instance vInst = validationSet.instance(i);

								if (classifyingBinary && vInst.classIsMissing()) {
									continue;
								}

								Instance classMissing = (Instance) vInst.copy();
								classMissing.setDataset(vInst.dataset());
								if (!(baseClassifier instanceof ClassificationViaClustering)) {

									classMissing.setClassMissing();
								}
								// if (!classifyingBinary) {
								for (int p = 0; p < Config.LABELS_SINGLES.length; ++p) {
									classMissing
											.setValue(
													prevLabelStart + p,
													(!classifyingBinary && p == prevInstLabel) ? 1
															: 0);
								}

								double[] vClassDist = featSelector
										.distributionForInstance(classMissing);
								if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
									if (l == 0) {
										instDistribs[i][0] = vClassDist[Config.LABLES_BINARY_POSITIVE_IX];
										for (int c = 1; c < instDistribs[i].length; ++c) {
											instDistribs[i][c] = vClassDist[Config.LABLES_BINARY_NEGATIVE_IX];
										}
									} else if (l == 1) {
										// Condition on the positive
										// classification
										for (int c = 1; c < 4 /*
															 * instDistribs[i].
															 * length
															 */; ++c) {
											int p = (c > 0 && c < 4 ? Config.LABLES_BINARY_POSITIVE_IX
													: Config.LABLES_BINARY_NEGATIVE_IX);

											double numerator = vClassDist[p]
													* instDistribs[i][c];
											double denim = vClassDist[p]
													+ instDistribs[i][c];
											if (denim != 0) {
												instDistribs[i][c] = numerator
														/ denim;
											} else {
												// bith are already zeros
											}
										}
									} else if (l == 2 || l == 3) {

										int start = 0, end = 0;

										if (l == 2) {
											start = 1;
											end = 4;
										} else {
											start = 4;
											end = 11;
										}
										for (int c = start; c < end; ++c) {
											double numerator = vClassDist[c]
													* instDistribs[i][c];
											double denim = vClassDist[c]
													+ instDistribs[i][c];
											if (denim != 0) {
												instDistribs[i][c] = numerator
														/ denim;
											} // else they are both already
												// zeros
										}
									}

									if (l == 3) {
										MathUtil.normalizeProbabilities(instDistribs[i]);
										vClassDist = instDistribs[i];
//										// only till 4.. that's cheating!!
//										for (int c = 0; c < 4; ++c) {
//											vClassDist[c] = instDistribs[i][c];
//										}
//										MathUtil.normalizeProbabilities(vClassDist);
									}
								}
								// FIXMENOT: use the actual instance ID
								String instId = Double.toString((i * 1.0 + 1));
								// Double.toString(classMissing
								// .value(0)); // Double.toString((i + 1) *
								// // 1.0)
								String placeId = instIdPlaceId
										.getProperty(instId);
								featSelectWr.append(placeId);
								// vInst.dataset()C.relationName() + "[" + (i +
								// "]");
								// double vClassMaxProb =
								// Double.NEGATIVE_INFINITY;
								double vClass = getClassFromDistrib(vClassDist,
										classMissing, vInst.classValue());
								for (int j = 0; j < vClassDist.length; ++j) {
									featSelectWr.append("\t" + vClassDist[j]);
									// if (vClassDist[j] > vClassMaxProb) {
									// vClassMaxProb = vClassDist[j];
									// vClass = j;
									// }
								}

								featSelectWr
										.append('\t')
										.append(Double.toString(vInst
												.classValue())).append('\n');
								synchronized (placeFreqMap) {
									Map<String, Frequency> placeFrequencyMap = placeFreqMap
											.get(eval.getClass().getName());
									if (placeFrequencyMap == null) {
										placeFrequencyMap = new TreeMap<String, Frequency>();
										placeFreqMap.put(eval.getClass()
												.getName(), placeFrequencyMap);
									}
									Frequency placeFreq = placeFrequencyMap
											.get(placeId);
									if (placeFreq == null) {
										placeFreq = new Frequency();
										placeFrequencyMap.put(placeId,
												placeFreq);
									}
									placeFreq.addValue(Long.toString(Math
											.round(vClass)));
								}
								String line = instIDVisitTimeRd.readLine();
								if (!line.startsWith(validationSet
										.relationName() + "\t" + instId)) {
									throw new AssertionError(
											"Visit times and labels are not inline");
								}
								instIDVisitTimeWr.append(line + "\t" + vClass
										+ "\t" + vInst.classValue() + "\n");
								prevInstLabel = vClass;
								// // The class "Value" is actually its
								// index!!!!!!
								// if (vClass == vInst.classValue()) {
								// ++featSelectCorrectCount;
								// }
								if (vClass == vInst.classValue()) {
									// // don't report accuracy of 0
									// && vInst.classValue() != 0) {
									if (classifyingBinary) {
										if (// +1 going to a leaf
										(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length == 3)
												// -1 going to a leaf
												|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length == 3)) {

											// // there is only 1 +C+
											// String actualC =
											// validationActualCs
											// .getProperty(Long.toString(Math
											// .round(vInst.value(0))));
											// if (pSplits[1].equals(actualC)) {
											++featSelectCorrectCount;

										} else if (// +1 going to a non-leaf
										(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length > 3)
												// -1 going to a non-leaf
												|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length > 3)) {

											// No meaning for accuracy at non
											// leaves
											// ++trueClassificationsCount;
											++trueInternalDirectionCount;
										}
									} else {
										++featSelectCorrectCount;
									}
								}
								if (classifyingBinary) {
									// Count number that should go to a class
									// leaf or get directed to a subtree
									if (vInst.classValue() == 0) {
										// +1
										if (pSplits.length == 3) {
											++featSelTotalCount;
										} else {
											++totalInternalDirections;
										}
									} else {
										// -1
										if (mSplits.length == 3) {
											++featSelTotalCount;
										} else {
											++totalInternalDirections;
										}
									}
								}
								long trueLabelCfMIx;
								if (classifyingBinary) {
									try {
										// Get the true class from the
										// properties
										// file
										// Using the .. oh, why did I copy
										// paste..look
										// above
										// :(
										// 0 is the index of the ID attrib
										if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
											trueLabelCfMIx = Math.round(insts
													.getValue()[0].instance(i)
													.classValue());
										} else {
											trueLabelCfMIx = Long
													.parseLong(validationActualCs.getProperty(Double
															.toString(i * 1.0 + 1)));
											// // Long.toString(Math.round(\
											// vInst.value(0))));
										}
									} catch (NumberFormatException ignored) {
										trueLabelCfMIx = 0;
										System.err
												.println(baseClassifierClazz
														.getSimpleName()
														+ " - "
														+ (System
																.currentTimeMillis() - startTime)
														+ " (fold "
														+ v
														+ "): Cannot get true label for instance ID "
														// + vInst.value(0)
														+ (i + 1)
														+ " in positive class "
														+ positiveClass);
									}

								} else {
									trueLabelCfMIx = Math.round(vInst
											.classValue());
								}
								long bestLabelInt = Math.round(vClass);

								if ((!Config.CLASSIFY_SEPARATE_PREVALENCE)
										|| (Config.CLASSIFY_SEPARATE_PREVALENCE && l < 2)) {
									foldFeactSelectCM[(int) trueLabelCfMIx]
											.addValue((classifyingBinary ? vClass == (vInst
													.classValue())
													: bestLabelInt));
								}
								// foldFeactSelectCM[(int) trueLabelCfMIx]
								// .addValue(bestLabelInt);
								if ((!Config.CLASSIFY_SEPARATE_PREVALENCE)
										|| (Config.CLASSIFY_SEPARATE_PREVALENCE && l < 2)) {
									synchronized (totalFeatSelectCM) {
										totalFeatSelectCM.get(positiveClass)
												.get(eval.getClass() // attrSelectEvalClazz
														.getName() /*
																	 * +
																	 * searchClazz
																	 * . getName
																	 */)[(int) trueLabelCfMIx]
												// .addValue(bestLabelInt);
												.addValue((classifyingBinary ? vClass == (vInst
														.classValue())
														: bestLabelInt));
									}
								}
							}
						} finally {
							instIDVisitTimeRd.close();
							instIDVisitTimeWr.flush();
							instIDVisitTimeWr.close();
						}

						if (featSelTotalCount > 0) {
							// a map of accuracies for different algot
							accuracyMap.put(eval.getClass() // attrSelectEvalClazz
									.getName(), featSelectCorrectCount * 1.0
									/ featSelTotalCount);
							synchronized (cvFeatSelectAccuracyWr) {

								cvFeatSelectAccuracyWr
										.get(positiveClass)
										.get(eval.getClass() // attrSelectEvalClazz
												.getName() /*
															 * + searchClazz .
															 * getName
															 */)
										.append(Integer.toString(v))
										.append('\t')
										.append(Integer
												.toString(featSelectCorrectCount))
										.append('\t')
										.append(Integer
												.toString(featSelTotalCount))
										.append('\t')
										.append(Double.toString(accuracyMap
												.get(eval.getClass() // attrSelectEvalClazz
														.getName())))
										.append('\n');
							}
						}
						if (totalInternalDirections > 0) {
							// a map of accuracies for different algot
							accuracyMap.put(INTERNAL_DIR_PFX + eval.getClass() // attrSelectEvalClazz
									.getName(), trueInternalDirectionCount
									* 1.0 / totalInternalDirections);
							synchronized (cvFeatSelectAccuracyWr) {

								cvFeatSelectAccuracyWr
										.get(positiveClass)
										.get(eval.getClass() // attrSelectEvalClazz
												.getName() /*
															 * + searchClazz .
															 * getName
															 */)
										.append(INTERNAL_DIR_PFX
												+ Integer.toString(v))
										.append('\t')
										.append(Integer
												.toString(trueInternalDirectionCount))
										.append('\t')
										.append(Integer
												.toString(totalInternalDirections))
										.append('\t')
										.append(Double.toString(accuracyMap
												.get(INTERNAL_DIR_PFX
														+ eval.getClass() // attrSelectEvalClazz
																.getName())))
										.append('\n');
							}
						}
					}
				} catch (WekaException ignored) {
					if (ignored.getMessage().startsWith(
							"Not enough training instances")) {
						featSelectWr.append(ignored.getMessage());
						return; // continue;
					}
				} catch (Exception ignored) {
					ignored.printStackTrace(System.err);
					return; // continue;
				} finally {
					featSelectWr.flush();
					featSelectWr.close();
				}

				if (featSelector == null) {
					System.err.println("featSelector is null for "
							+ baseClassifier.getClass().getName()
							+ " "
							+ (eval != null ? eval.getClass().getName()
									: "eval is also null") + " " + filenamePfx);
				} else if (eval == null) {
					System.err.println("eval is null for "
							+ baseClassifier.getClass().getName() + " "
							+ featSelector + " " + filenamePfx);
				} else {
					FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
							baseClassifier.getClass().getName(), eval
									.getClass() // attrSelectEvalClazz
									.getName(), filenamePfx
									+ "_feat-selection.txt"), featSelector
							.toString());
				}
				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifier.getClass().getName(), eval.getClass() // attrSelectEvalClazz
								.getName(), filenamePfx + "_" + positiveClass
								+ "_tie-size.txt"), tieSize.toString());
				Writer tieWr = Channels.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(outputPath, baseClassifier
										.getClass().getName(), eval.getClass() // attrSelectEvalClazz
										.getName(), filenamePfx + "_"
										+ positiveClass + "_near-ties.txt"))
								.getChannel(), Config.OUT_CHARSET);
				try {
					tieWr.append("Random\tPrevious\tTie-Count\tTotal-Count\tRand-Correct\tPrev-Correct\tRand-Wrong\tPrev-Wrong\n");
					long totalTies = nearTies.getCount('R')
							+ nearTies.getCount('P');
					tieWr.append(Double.toString(nearTies.getPct('R')))
							.append('\t')
							.append(Double.toString(nearTies.getPct('P')))
							.append('\t')
							.append(Long.toString(totalTies))
							.append('\t')
							.append(Long.toString(validationSet.numInstances()))
							.append('\t')
							.append(Double.toString(randLabelCorrect
									.getPct(Boolean.TRUE)))
							.append('\t')
							.append(Double.toString(prevLabelCorrect
									.getPct(Boolean.TRUE)))
							.append('\t')
							.append(Double.toString(randLabelCorrect
									.getPct(Boolean.FALSE)))
							.append('\t')
							.append(Double.toString(prevLabelCorrect
									.getPct(Boolean.FALSE))).append('\n');
				} finally {
					tieWr.flush();
					tieWr.close();
				}
				// algo name
				System.out.println(baseClassifierClazz.getSimpleName() + "/"
						+ eval.getClass() // attrSelectEvalClazz
								.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished feature selection for "
						+ filenamePfx);

				if (Config.CLASSIFYFEATSEL_WRITE_TRANSFORMED
						&& eval instanceof PrincipalComponents) {
					// TODO will this be effective after the fact?
					((PrincipalComponents) eval).setMaximumAttributeNames(-1);

					System.out
							.println(baseClassifierClazz.getSimpleName()
									+ "/"
									+ eval.getClass() // attrSelectEvalClazz
											.getSimpleName()
									+ " - "
									+ (System.currentTimeMillis() - startTime)
									+ " (fold "
									+ v
									+ "): Writing out transformed validation instance for "
									+ filenamePfx);
					OutputStream trOut = FileUtils.openOutputStream(FileUtils
							.getFile(inPath, "transformed", baseClassifier
									.getClass().getName(), eval.getClass() // attrSelectEvalClazz
									.getName(), positiveClass,
									validationSet.relationName() + ".arff"));
					try {
						Instances transformedSet = ((AttributeTransformer) eval)
								.transformedData(validationSet);
						ArffSaver trSaver = new ArffSaver();
						trSaver.setDestination(trOut);
						trSaver.setInstances(transformedSet);
						trSaver.writeBatch();
					} catch (Exception ignored) {
						ignored.printStackTrace();
					} finally {
						trOut.flush();
						trOut.close();
					}
				}
			}
		}
	}

	private static String outputPath = "C:\\mdc-datasets\\weka\\validation_sample-noweight_cascade-j48_condition";
	private String inPath = "C:\\mdc-datasets\\weka\\segmented_user_sample-noweight";
	private static String trainingPath = "C:\\mdc-datasets\\weka\\segmented_user";
	private static String testPath = "C:\\mdc-datasets\\weka\\segmented_user_test";

	private final Map<String, Frequency[]> totalConfusionMatrix;
	private final Map<String, Map<String, Frequency[]>> totalFeatSelectCM;
	private long startTime = System.currentTimeMillis();

	// private final Map<String, Frequency> placeFrequencyMap =
	// Collections.synchronizedMap(new TreeMap<String, Frequency>());
	private final Map<String, Map<String, Frequency>> placeFreqMap = Collections
			.synchronizedMap(new HashMap<String, Map<String, Frequency>>());
	private final Map<String, Writer> cvClassificationAccuracyWr;
	private final Map<String, Map<String, Writer>> cvFeatSelectAccuracyWr;

	private Class<? extends Classifier> baseClassifierClazz;
	private final boolean ignoreInstsWithMissingClass;
	private final boolean classifyBinaryHierarchy;
	private final boolean classifyMultiClass;
	private final boolean featSelect;
	// private final Class[] attrSelectEvaluationClazzes;

	private File[] classHierDirs;
	private double acrossPCsAllFeatAccuNumer;
	private double acrossPCsAllFeatAccuDenim;

	private final Class attSelClazz;

	public ClassifyAndFeatSelect(
			Class<? extends Classifier> baseClassifierClazz,
			boolean pClassifyMultiClass, boolean pClassifyBinaryHierarchy,
			boolean pFeatSelect, Class pAttrSelectEvaluationClazzes,
			boolean pIgnoreInstsWithMissingClass) throws IOException {
		this.classifyBinaryHierarchy = pClassifyBinaryHierarchy;
		this.classifyMultiClass = pClassifyMultiClass;
		this.ignoreInstsWithMissingClass = pIgnoreInstsWithMissingClass;
		this.featSelect = pFeatSelect;
		// this.attrSelectEvaluationClazzes = pAttrSelectEvaluationClazzes;
		this.attSelClazz = pAttrSelectEvaluationClazzes;
		classHierDirs = FileUtils.getFile(inPath).listFiles(
				new FilenameFilter() {

					@Override
					public boolean accept(File arg0, String arg1) {
						boolean result = false;
						if (classifyBinaryHierarchy) {
							result |= arg1.startsWith("c");
						}
						if (classifyMultiClass) {
							result |= arg1
									.equals(Config.LABELS_MULTICLASS_NAME);
						}
						return result;
					}
				});

		this.baseClassifierClazz = baseClassifierClazz;

		totalConfusionMatrix = Collections
				.synchronizedMap(new HashMap<String, Frequency[]>());
		cvClassificationAccuracyWr = Collections
				.synchronizedMap(new HashMap<String, Writer>());
		totalFeatSelectCM = Collections
				.synchronizedMap(new HashMap<String, Map<String, Frequency[]>>());
		cvFeatSelectAccuracyWr = Collections
				.synchronizedMap(new HashMap<String, Map<String, Writer>>());

		for (File positiveClassDir : classHierDirs) {
			String positiveClass = positiveClassDir.getName();

			Frequency[] pcCM = new Frequency[Config.LABELS_SINGLES.length];
			for (int i = 0; i < pcCM.length; ++i) {
				pcCM[i] = new Frequency();
			}
			totalConfusionMatrix.put(positiveClass, pcCM);

			cvClassificationAccuracyWr
					.put(positiveClass,
							Channels.newWriter(
									FileUtils
											.openOutputStream(
													FileUtils
															.getFile(
																	outputPath,
																	baseClassifierClazz
																			.getName(),
																	positiveClass
																			+ "_classification-accuracy-cv.txt"))
											.getChannel(), Config.OUT_CHARSET));
			cvClassificationAccuracyWr.get(positiveClass).append("vFold")
					.append('\t').append("trueN").append('\t').append("totalN")
					.append('\t').append("accuracy").append('\n');

			Map<String, Frequency[]> pcTotalFeatSelectCM = Collections
					.synchronizedMap(new HashMap<String, Frequency[]>());

			Map<String, Writer> pcCvFeatSelectAccuracyWr = Collections
					.synchronizedMap(new HashMap<String, Writer>());
			// for (@SuppressWarnings("rawtypes")
			// Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
			Class attrSelectEvalClazz = attSelClazz;
			Frequency[] freqArr = new Frequency[Config.LABELS_SINGLES.length];
			for (int i = 0; i < freqArr.length; ++i) {
				freqArr[i] = new Frequency();
			}
			pcTotalFeatSelectCM.put(attrSelectEvalClazz.getName() /*
																 * + searchClazz
																 */, freqArr);

			Writer writer = Channels
					.newWriter(
							FileUtils
									.openOutputStream(
											FileUtils
													.getFile(
															outputPath,
															baseClassifierClazz
																	.getName(),
															attrSelectEvalClazz
																	.getName() /*
																				 * +
																				 * searchClazz
																				 */,
															"n"
																	+ superiorityRatio
																	+ "_feat-selected-accuracy-cv.txt"))
									.getChannel(), Config.OUT_CHARSET);

			writer.append("vFold").append('\t').append("trueN").append('\t')
					.append("totalN").append('\t').append("accuracy")
					.append('\n');
			pcCvFeatSelectAccuracyWr.put(attrSelectEvalClazz.getName() /*
																		 * +
																		 * searchClazz
																		 */,
					writer);

			// }
			totalFeatSelectCM.put(positiveClass, pcTotalFeatSelectCM);
			cvFeatSelectAccuracyWr.put(positiveClass, pcCvFeatSelectAccuracyWr);
		}

	}

	static List<Integer> vArr = new LinkedList<Integer>();

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig,
				"ClassifyAndFeatSelect");
		try {
			System.setErr(new PrintStream(notifyStream));

			Config.placeLabels = new Properties();
			try {
				Config.placeLabels.load(FileUtils.openInputStream(FileUtils
						.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

				Config.quantizedFields = new Properties();
				Config.quantizedFields.load(FileUtils.openInputStream(FileUtils
						.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}

			Random rand = new Random(System.currentTimeMillis());
			// for (int i = 0; i < Config.VALIDATION_SAMPLE_USERS; ++i) {
			// int r;
			// do {
			// r = rand.nextInt(Config.NUM_USERS_TO_PROCESS);
			// } while (i > 0 && vArr.contains(r));
			// vArr.add(r);
			// }
			vArr = Arrays.asList(8, 24, 5, 9, 11, 19, 4, 6, 64, 46, 78, 68, 66,
					58, 12, 57, 54, 74, 30, 1);

			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
			// baseClassifierClazz.getName(), positiveClass
					"sample-users.csv"), vArr.toString());

			// if (validate) {
			// CalcCutPoints.main(args);
			// ImportIntoMallet.main(args);
			//
			// Still cannot handle quantized vals
			// CountConditionalFreqs countCond = new
			// CountConditionalFreqs();
			// ExecutorService countExec =
			// Executors.newSingleThreadExecutor();
			// countExec.submit(countCond);

			// LoadCountsAsAttributes.main(args);

			ClassifyAndFeatSelect app;

			ExecutorService appExec = Executors
			// .newSingleThreadExecutor();
					.newFixedThreadPool(Config.NUM_THREADS);
			Future<Void> lastFuture = null;

			for (Class attrSelectEvalClazz : attrSelectorsForMain) {

				// try {
				// // SVM
				// app = new ClassifyAndFeatSelect(LibSVM.class, false, true,
				// true, new Class[] { SVMAttributeEval.class }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// // Cannot handle multinomial attrs
				// // Bayesian Logisitc Regression
				// app = new
				// ClassifyAndFeatSelect(BayesianLogisticRegression.class);
				// app.call();
				// Still says cannot handle missing values.. where are they?
				// try {
				// app = new ClassifyAndFeatSelect(NaiveBayesMultinomial.class,
				// true, true, true, new Class[] {
				// GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				try {
					// for (int n = 1; n <= 10; ++n) {
					// // The king
					app = new ClassifyAndFeatSelect(RandomForest.class, true,
							false, true, attrSelectEvalClazz, false);
					// new Class[] {
					// GainRatioAttributeEval.class,
					// // // LatentSemanticAnalysis.class,
					// PrincipalComponents.class, }, false);
					// app.n = n;
					// app.call();
					lastFuture = appExec.submit(app);
					// }
				} catch (Exception e) {
					// catch block
					e.printStackTrace();
				}
				// try {
				// // Boosting
				// app = new ClassifyAndFeatSelect(AdaBoostM1.class, true,
				// false, true, attrSelectEvalClazz,
				// // new Class[] { GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, },
				// false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				try {
					// C4.5 decision tree
					app = new ClassifyAndFeatSelect(J48.class, true, false,
							true, attrSelectEvalClazz, false);
					// new Class[] { GainRatioAttributeEval.class,
					// // LatentSemanticAnalysis.class,
					// PrincipalComponents.class, }, false);
					lastFuture = appExec.submit(app);
					// app.call();
				} catch (Exception e) {
					// catch block
					e.printStackTrace();
				}
				// // TODONE try these
				// try {
				// // OK
				// app = new ClassifyAndFeatSelect(REPTree.class, true, false,
				// true, attrSelectEvalClazz, false);
				// // new Class[] { GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // OK
				// app = new ClassifyAndFeatSelect(SimpleCart.class, true,
				// false, true, attrSelectEvalClazz, false);
				// // new Class[] { GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // Naive Bayes
				// app = new ClassifyAndFeatSelect(NaiveBayesUpdateable.class,
				// true, false, true, attrSelectEvalClazz, false);
				// // new Class[] {
				// // GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				//
				// try {
				// app = new ClassifyAndFeatSelect(MultiBoostAB.class, true,
				// false, true, attrSelectEvalClazz, false);
				// // new Class[] {
				// // GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// //.ADTree: Cannot handle multi-valued nominal class
				// app = new ClassifyAndFeatSelect(ADTree.class, true,
				// false, true, new Class[] {
				// GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // Takes ofrever on only 8 users
				// app = new ClassifyAndFeatSelect(BFTree.class, true,
				// false, true, new Class[] {
				// GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // Takes ofrever on only 8 users
				// app = new ClassifyAndFeatSelect(FT.class, true,
				// false, true, new Class[] {
				// GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // Runs ok
				// app = new ClassifyAndFeatSelect(J48graft.class, true, false,
				// true, attrSelectEvalClazz, false);
				// // new Class[] { GainRatioAttributeEval.class,
				// // // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// try {
				// //OK, but takes forever on the whole dataset
				// app = new ClassifyAndFeatSelect(LADTree.class, true, false,
				// true,
				// new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // Forever
				// app = new ClassifyAndFeatSelect(LMT.class, true, false, true,
				// new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// // LinearRegression: Cannot handle multi-valued nominal
				// class!
				// app = new ClassifyAndFeatSelect(M5P.class, true, false, true,
				// new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// try {
				// /// Forever
				// app = new ClassifyAndFeatSelect(NBTree.class, true, false,
				// true,
				// new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// lastFuture = appExec.submit(app);
				// // app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// try {
				// // By clustering
				// app = new ClassifyAndFeatSelect(
				// ClassificationViaClustering.class, true, false,
				// true, attrSelectEvalClazz, false);
				// // new Class[] { GainRatioAttributeEval.class,
				// // // LatentSemanticAnalysis.class,
				// // PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// try {
				//
				// app = new ClassifyAndFeatSelect(MixtureDistribution.class,
				// false,
				// true,
				// true, new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// // Exception: weka.classifiers.functions.Logistic: Not enough
				// // training
				// // instances with class labels (required: 1, provided: 0)!
				// // Logistic Regression
				// try {
				//
				// app = new ClassifyAndFeatSelect(Logistic.class, false, true,
				// true, new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// app.call();
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				//
				// // // Bayes Net
				// try {
				// app = new ClassifyAndFeatSelect(BayesNet.class, true, false,
				// true, new Class[] { GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// // app.call();
				// lastFuture = appExec.submit(app);
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }

				// try {
				// // Neural networks
				// app = new ClassifyAndFeatSelect(MultilayerPerceptron.class,
				// true, true, true, new Class[] {
				// GainRatioAttributeEval.class,
				// // LatentSemanticAnalysis.class,
				// PrincipalComponents.class, }, false);
				// app.call();
				//
				// } catch (Exception e) {
				// // catch block
				// e.printStackTrace();
				// }
				// countExec.shutdown();
				// while (!countExec.isTerminated()) {
				// Thread.sleep(5000);
				// }
			}
			try {
				lastFuture.get();
			} catch (Exception e) {
				// catch block
				e.printStackTrace();
			}

			appExec.shutdown();
			while (!appExec.isTerminated()) {
				// System.out.println("Shutting down");
				Thread.sleep(5000);
			}
			// } else {
			// throw new UnsupportedOperationException();
			// // File trainingDir = FileUtils.getFile(trainingPath);
			// // ArrayList<File> trainingFiles = new ArrayList<File>();
			// // trainingFiles.addAll(Arrays.asList(trainingDir
			// // .listFiles(new FilenameFilter() {
			// //
			// // @Override
			// // public boolean accept(File dir, String name) {
			// //
			// // return name.endsWith(".arff");
			// // }
			// // })));
			// // Instances trainingSet =
			// // loadAugmentedInstances(trainingFiles);
			// //
			// // File testDir = FileUtils.getFile(testPath);
			// // ArrayList<File> testFiles = new ArrayList<File>();
			// // testFiles.addAll(Arrays.asList(testDir
			// // .listFiles(new FilenameFilter() {
			// //
			// // @Override
			// // public boolean accept(File dir, String name) {
			// //
			// // return name.endsWith(".arff");
			// // }
			// // })));
			// // Instances testSet = loadAugmentedInstances(testFiles);
			// }
			System.err.println("Finished running at " + new Date());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				notifyStream.close();
			} catch (IOException ignored) {

			}
			System.setErr(errOrig);
		}

	}

	public Void call() {

		Pattern positiveSplit = Pattern.compile("\\+");
		Pattern minusSplit = Pattern.compile("\\-");

		HashMap<String, SummaryStatistics> accuracySummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> accuracySummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();
		HashMap<String, SummaryStatistics> internalDirSummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> internalDirSummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();
		HashMap<String, SummaryStatistics> multicAccuracySummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> multicAccuracySummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();

		try {

			System.out.println(baseClassifierClazz.getSimpleName() + " - "
					+ new Date().toString()
					+ " (total): Validating with folds of width  " + 1 /*
																		 * FIXME:
																		 * Config
																		 * .
																		 * VALIDATION_FOLD_WIDTH
																		 */
					+ " users");
			ExecutorService foldExecutors = Executors
			// .newSingleThreadExecutor();
					.newFixedThreadPool(Config.NUM_THREADS);
			ArrayList<Future<KeyValuePair<String, HashMap<String, Double>>>> foldFutures = new ArrayList<Future<KeyValuePair<String, HashMap<String, Double>>>>();
			int numClassifyTasks = 0;
			for (File positiveClassDir : classHierDirs) {
				String positiveClass = positiveClassDir.getName();

				// Will lazy init those, coz they keep growing
				// HashMap<String, SummaryStatistics>
				// pcAccuracySummaryFeatSelected = new HashMap<String,
				// SummaryStatistics>();
				// for (@SuppressWarnings("rawtypes")
				// Class clazz : attrSelectEvaluationClazzes) {
				// pcAccuracySummaryFeatSelected.put(clazz.getName(),
				// new SummaryStatistics());
				// }
				// accuracySummaryFeatSelected.put(positiveClass,
				// pcAccuracySummaryFeatSelected);
				// accuracySummaryAllFeatures.put(positiveClass,
				// new SummaryStatistics());

				// for (int v = 0; v < Config.NUM_USERS_TO_PROCESS /*
				// * FIXME:
				// * VALIDATION_FOLDS
				// */; ++v) {

				synchronized (vArr) {

					for (Integer v : vArr) {

						ASEvaluation eval = (ASEvaluation) attSelClazz
								.getConstructor().newInstance();

						if (eval instanceof GainRatioAttributeEval) {
							((GainRatioAttributeEval) eval)
									.setMissingMerge(false);
						}

						ASSearch search;
						if (eval instanceof SubsetEvaluator) {
							search = new GreedyStepwise();
							// ((GreedyStepwise)
							// search).setSearchBackwards(true);
							// ((GreedyStepwise)
							// search).setNumToSelect(this.baseClassifier.)
							((GreedyStepwise) search).setGenerateRanking(true);
						} else {
							search = new Ranker();
						}

						int foldStart = v * 1; // FIXME:
												// Config.VALIDATION_FOLD_WIDTH;
						FoldCallable FoldCallable = new FoldCallable(
								positiveClassDir, v, attSelClazz, search, eval);
						// , loadFold(positiveClassDir,foldStart));
						foldFutures.add(foldExecutors.submit(FoldCallable));
						++numClassifyTasks;
					}
				}

			}
			for (int i = 0; i < numClassifyTasks; ++i) {
				try {
					KeyValuePair<String, HashMap<String, Double>> accuracies = foldFutures
							.get(i).get();
					if (accuracies == null) {
						// System.err
						// .println("ERROR: Null accuracies map returned by fold: "
						// + i
						// + " for classifier "
						// + this.baseClassifierClazz.getName());
						continue;
					}

					if (Config.LABELS_MULTICLASS_NAME.equals(accuracies
							.getKey())) {
						// Multiclass not part of the consensus
						gatherSummaryStats(accuracies,
								multicAccuracySummaryAllFeatures,
								multicAccuracySummaryFeatSelected, false);

					} else {
						if (positiveSplit.split(accuracies.getKey()).length == 3
								|| minusSplit.split(accuracies.getKey()).length == 3) {

							gatherSummaryStats(accuracies,
									accuracySummaryAllFeatures,
									accuracySummaryFeatSelected, false);
						} else {
							System.out
									.println("Happily skipping to gather accuracy for the node that don't lead to  a leaf: "
											+ accuracies.getKey());
						}
						gatherSummaryStats(accuracies,
								internalDirSummaryAllFeatures,
								internalDirSummaryFeatSelected, true);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			foldExecutors.shutdown();
			while (!foldExecutors.isTerminated()) {
				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime)
						+ " (total): shutting down");
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			for (Writer wr : cvClassificationAccuracyWr.values()) {
				try {
					wr.flush();
					wr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			for (Map<String, Writer> pcCvFeatSelectAccuracyWr : cvFeatSelectAccuracyWr
					.values()) {
				for (Writer wr : pcCvFeatSelectAccuracyWr.values()) {
					try {
						wr.flush();
						wr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}
		}
		if (classifyMultiClass) {
			// There is no internal node, it's just multi-class
			try {
				printSummaryStats(Config.LABELS_MULTICLASS_NAME,
						multicAccuracySummaryAllFeatures
								.get(Config.LABELS_MULTICLASS_NAME),
						multicAccuracySummaryFeatSelected
								.get(Config.LABELS_MULTICLASS_NAME), null,
						null, true, false);

				// TODO: the same for binary.. but meh!
				printFrequency(ALL_FEATS);
				// for (Class featSelClazz : attrSelectEvaluationClazzes) {
				printFrequency(attSelClazz.getName());
				// }
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (classifyBinaryHierarchy) {
			// keep the setting to 0 here,
			// they could be messed up from earlier
			acrossPCsAllFeatAccuNumer = 0.0;
			acrossPCsAllFeatAccuDenim = 0.0;
			for (File positiveClassDir : classHierDirs) {
				String positiveClass = positiveClassDir.getName();
				if (Config.LABELS_MULTICLASS_NAME.equals(positiveClass)) {
					// Multiclass is not part of the ensemble
					continue;
				}
				SummaryStatistics pcAccuSummaryAllFeats = accuracySummaryAllFeatures
						.get(positiveClass);
				HashMap<String, SummaryStatistics> pcAccuracySummaryFeatSelected = accuracySummaryFeatSelected
						.get(positiveClass);
				SummaryStatistics pcInternalDirSummaryAllFeatures = internalDirSummaryAllFeatures
						.get(positiveClass);
				HashMap<String, SummaryStatistics> pcInternalDirSummaryFeatSelected = internalDirSummaryFeatSelected
						.get(positiveClass);

				try {
					printSummaryStats(
							positiveClass,
							pcAccuSummaryAllFeats,
							pcAccuracySummaryFeatSelected,
							pcInternalDirSummaryAllFeatures,
							pcInternalDirSummaryFeatSelected,
							(positiveSplit.split(positiveClass).length == 3 || minusSplit
									.split(positiveClass).length == 3), true);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			try {
				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifierClazz.getName(),
						"binary-ensemble_consensus_accuracy-summary.txt"),
						Double.toString(acrossPCsAllFeatAccuNumer
								/ acrossPCsAllFeatAccuDenim));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println(baseClassifierClazz.getSimpleName() + " - "
				+ new Date().toString() + " (total): Done in "
				+ (System.currentTimeMillis() - startTime) + " millis");
		return null;
	}

	public static void writeConfusionMatrix(Writer foldConfusionWr,
			Frequency[] foldConfusionMatrix, boolean classifyingBinary)
			throws IOException {
		if (foldConfusionMatrix.length == 0) {
			foldConfusionWr.append("EMPTY");
			return;
		}
		if (!classifyingBinary) {
			foldConfusionWr.append("label\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\n");
		} else {
			foldConfusionWr.append("label\tTRUE\tFALSE\n");
			// Iterator<Comparable<?>> valsIster = foldConfusionMatrix[0]
			// .valuesIterator();
			// boolean headerTrue = false;
			// boolean headerFalse = false;
			// while (valsIster.hasNext()) {
			// if((Boolean)valsIster.next()){
			// foldConfusionWr.append('\t').append("TRUE");
			// headerTrue = true;
			// } else {
			// foldConfusionWr.append('\t').append("FALSE");
			// headerFalse = true;
			// }
			// }
			// if(!headerTrue){
			// foldConfusionWr.append('\t').append("TRUE");
			// }
			// if(!headerFalse){
			// foldConfusionWr.append('\t').append("FALSE");
			// }
			//
		}

		for (int i = 0; i < foldConfusionMatrix.length; ++i) {
			foldConfusionWr.append(Integer.toString(i));
			long totalCount = 0;

			if (!classifyingBinary) {
				for (int j = 0; j < Config.LABELS_SINGLES.length; ++j) {
					long cnt = foldConfusionMatrix[i].getCount(j); // valsIster.next());
					totalCount += cnt;
					foldConfusionWr.append('\t').append(Long.toString(cnt));
				}
			} else {
				// valsIster = foldConfusionMatrix[i].valuesIterator();
				// while (valsIster.hasNext()) {
				long cnt = foldConfusionMatrix[i].getCount(Boolean.TRUE);
				totalCount += cnt;
				foldConfusionWr.append('\t').append(Long.toString(cnt));
				cnt = foldConfusionMatrix[i].getCount(Boolean.FALSE);
				totalCount += cnt;
				foldConfusionWr.append('\t').append(Long.toString(cnt));
			}
			foldConfusionWr.append('\t').append(Long.toString(totalCount))
					.append('\n');
		}
	}

	void gatherSummaryStats(
			KeyValuePair<String, HashMap<String, Double>> accuracies,
			HashMap<String, SummaryStatistics> accuracySummaryAllFeatures,
			HashMap<String, HashMap<String, SummaryStatistics>> accuracySummaryFeatSelected,
			boolean internal) {

		Double accu = accuracies.getValue().get(
				(internal ? INTERNAL_DIR_PFX : "") + ALL_FEATS);
		if (accu == null) {
			// System.err.println("ERROR: Null accuracy for feat selector "
			// + (internal ? INTERNAL_DIR_PFX : "") + ALL_FEATS
			// + " for classifier " + this.baseClassifierClazz.getName()
			// + " in positive class: " + accuracies.getKey());
		} else {
			SummaryStatistics accSumm = accuracySummaryAllFeatures
					.get((internal ? INTERNAL_DIR_PFX : "")
							+ accuracies.getKey());
			if (accSumm == null) {
				accSumm = new SummaryStatistics();
				accuracySummaryAllFeatures.put(
				// No need.. the map will be different already (internal ?
				// INTERNAL_DIR_PFX: "") +
						accuracies.getKey(), accSumm);
			}
			accSumm.addValue(accu);
		}

		// for (@SuppressWarnings("rawtypes")
		// Class clazz : attrSelectEvaluationClazzes) {
		Class clazz = attSelClazz;
		accu = accuracies.getValue().get(
				(internal ? INTERNAL_DIR_PFX : "") + clazz.getName());
		if (accu == null) {
			// System.err.println("ERROR: Null accuracy for feat selector "
			// + clazz.getName() + " for classifier "
			// + this.baseClassifierClazz.getName()
			// + " in positive class: " + accuracies.getKey());
		} else {
			HashMap<String, SummaryStatistics> accSummMap = accuracySummaryFeatSelected
					.get(accuracies.getKey());
			if (accSummMap == null) {
				accSummMap = new HashMap<String, SummaryStatistics>();
				accuracySummaryFeatSelected
						.put(accuracies.getKey(), accSummMap);
			}

			SummaryStatistics accSumm = accSummMap
					.get((internal ? INTERNAL_DIR_PFX : "") + clazz.getName());
			if (accSumm == null) {
				accSumm = new SummaryStatistics();
				accuracySummaryFeatSelected.get(accuracies.getKey()).put(
				// No need.. the map will be different already (internal ?
				// INTERNAL_DIR_PFX : "") +
						clazz.getName(), accSumm);
			}
			accSumm.addValue(accu);
		}
		// }
	}

	void printFrequency(String key) throws IOException {
		Map<String, Frequency> freqMap = placeFreqMap.get(key);
		if (freqMap == null) {
			// System.err.println();
			return;
		}
		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath,
								baseClassifierClazz.getName(),// positiveClass
								key, "placeid-predictions.csv")).getChannel(),
				Config.OUT_CHARSET);
		try {
			wr.append("placeid\tL0\tL1\tL2\tL3\tL4\tL5\tL6\tL7\tL8\tL9\tL10\total\n");
			for (String placeid : freqMap.keySet()) {
				wr.append(placeid);
				Frequency freq = freqMap.get(placeid);
				int total = 0;
				for (String label : Config.LABELS_SINGLES) {
					wr.append('\t').append(Double.toString(freq.getPct(label)));
					total += freq.getCount(label);
				}
				wr.append("\t" + total).append('\n');
			}

		} finally {
			wr.flush();
			wr.close();
		}
	}

	void printSummaryStats(
			String positiveClass,
			SummaryStatistics pcAccuSummaryAllFeats,
			HashMap<String, SummaryStatistics> pcAccuracySummaryFeatSelected,
			SummaryStatistics pcInternalDirSummaryAllFeatures,
			HashMap<String, SummaryStatistics> pcInternalDirSummaryFeatSelected,
			boolean expectAccuracy, boolean expectInternal) throws IOException {

		if (expectInternal) {
			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifierClazz.getName(), positiveClass
							+ "_internal-dir-summary.txt"),
					pcInternalDirSummaryAllFeatures.toString());
		}

		if (expectAccuracy) {
			if (pcAccuSummaryAllFeats != null) {
				FileUtils.writeStringToFile(
						FileUtils.getFile(outputPath,
								baseClassifierClazz.getName(), positiveClass
										+ "_accuracy-summary.txt"),
						pcAccuSummaryAllFeats.toString());

				// The Mean of Means.. the variance would be from the t distrib
				// But many accuracies have n = 0, thus we shouldn't increase
				// denim
				// coz those means are meaningless
				if (pcAccuSummaryAllFeats.getN() > 0) {
					acrossPCsAllFeatAccuNumer += pcAccuSummaryAllFeats
							.getMean();
					++acrossPCsAllFeatAccuDenim;
				}

				// // Grand Mean
				// acrossPCsAllFeatAccuNumer += pcAccuSummaryAllFeats.getSum();
				// //pcAccuSummaryAllFeats.getN() *
				// pcAccuSummaryAllFeats.getMean();
				// acrossPCsAllFeatAccuDenim += pcAccuSummaryAllFeats.getN();

				// I don't know what should I do when variance is 0..
				// /**
				// * The Graybill-Deal method as explained in
				// * http://www.itl.nist.gov/div898/software
				// * /dataplot/refman1/auxillar/consmean.htm
				// */
				// This is wrong.. tha division must be before the addition
				// double var = pcAccuSummaryAllFeats.getVariance();
				// if(var > 0){
				// acrossPCsAllFeatAccuNumer/=var;
				// acrossPCsAllFeatAccuDenim/=var;
				// }
			} else {
				// System.err
				// .println("pcAccuSummaryAllFeats is null for positive class:"
				// + positiveClass
				// + " and base classifier: "
				// + baseClassifierClazz.getName());
			}

		} else {
			System.out.println("Happily skipping accuracy for positive class: "
					+ positiveClass);
		}

		// for (@SuppressWarnings("rawtypes")
		// Class clazz : attrSelectEvaluationClazzes) {
		Class clazz = attSelClazz;
		if (expectAccuracy) {
			if (pcAccuracySummaryFeatSelected == null) {
				// System.err.println("Null accuracy map for "
				// + baseClassifierClazz.getSimpleName()
				// + " in positive class: " + positiveClass);
			} else if (!pcAccuracySummaryFeatSelected.containsKey(clazz
					.getName())) {
				//
				// System.err.println("Null summary for "
				// + baseClassifierClazz.getSimpleName()
				// + positiveClass + "/" + clazz.getSimpleName());

			} else {
				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifierClazz.getName(), clazz.getName(), "n"
								+ superiorityRatio + "_" + positiveClass
								+ "_feat-selected_accuracy-summary.txt"),
						pcAccuracySummaryFeatSelected.get(clazz.getName())
								.toString());
			}
		} else {
			System.out.println("Happily skipping accuracy for positive class: "
					+ positiveClass);
		}
		if (expectInternal) {
			if (pcInternalDirSummaryFeatSelected == null) {
				System.err.println("Null InternalDir map for "
						+ baseClassifierClazz.getSimpleName() + positiveClass);
			} else if (!pcInternalDirSummaryFeatSelected.containsKey(clazz
					.getName())) {

				System.err.println("Null summary for "
						+ baseClassifierClazz.getSimpleName() + positiveClass
						+ "/" + clazz.getSimpleName());

			} else {
				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifierClazz.getName(), clazz.getName(), "n"
								+ superiorityRatio + "_" + positiveClass
								+ "_feat-selected_internal-dir-summary.txt"),
						pcInternalDirSummaryFeatSelected.get(clazz.getName())
								.toString());
			}
		}
		// }

		Writer totalConfusionWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath,
								baseClassifierClazz.getName(), positiveClass
										+ "_confusion-matrix.txt"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(totalConfusionWr,
					totalConfusionMatrix.get(positiveClass),
					!Config.LABELS_MULTICLASS_NAME.equals(positiveClass));
		} finally {
			totalConfusionWr.flush();
			totalConfusionWr.close();
		}

		// for (@SuppressWarnings("rawtypes")
		// Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
		Class attrSelectEvalClazz = attSelClazz;
		Writer featSelectedConfusionWr = Channels
				.newWriter(
						FileUtils
								.openOutputStream(
										FileUtils
												.getFile(
														outputPath,
														baseClassifierClazz
																.getName(),
														attrSelectEvalClazz
																.getName(),
														/*
														 * searchClazz.
														 * getName()
														 */
														"n"
																+ superiorityRatio
																+ "_"
																+ positiveClass
																+ "_feat-selected-confusion-matrix.txt"))
								.getChannel(), Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(featSelectedConfusionWr, totalFeatSelectCM
					.get(positiveClass).get(attrSelectEvalClazz.getName()/*
																		 * +
																		 * searchClazz
																		 * .
																		 * getName
																		 */),
					!Config.LABELS_MULTICLASS_NAME.equals(positiveClass));
		} finally {
			featSelectedConfusionWr.flush();
			featSelectedConfusionWr.close();
		}
		// }

	}

	static Map<String, KeyValuePair<Instances[], Instances[]>> foldInstanceMap = new WeakHashMap<String, KeyValuePair<Instances[], Instances[]>>();

	// Collections.synchronizedMap(new WeakHashMap<String,
	// KeyValuePair<Instances, Instances[]>>());

	static/* synchronized */KeyValuePair<Instances[], Instances[]> loadFold(
			File positiveClassDir, int foldStart) throws Exception {
		String key = positiveClassDir.getName() + foldStart;
		KeyValuePair<Instances[], Instances[]> result = null;
		while (true) {
			synchronized (foldInstanceMap) {
				if (foldInstanceMap.containsKey(key)) {
					result = foldInstanceMap.get(key);
				} else {
					result = new KeyValuePair();
					foldInstanceMap.put(key, null);
				}
			}
			if (result == null) {
				// This means that some other thread is loading this data
				Thread.sleep(5000);
			} else {
				if (result.getKey() != null) {
					// really some preloaded data
					return copyInsts(result);
				} else {
					// we will load this data
					break;
				}
			}
		}
		ArrayList<File> dataFiles = new ArrayList<File>();
		dataFiles.addAll(Arrays.asList(positiveClassDir
				.listFiles(new FilenameFilter() {

					@Override
					public boolean accept(File dir, String name) {

						return name.endsWith(".arff");
					}
				})));

		Instances[] validationSet = new Instances[1];// FIXME:
														// Config.VALIDATION_FOLD_WIDTH];
		for (int i = 0; i < 1 /* FIXME: Config.VALIDATION_FOLD_WIDTH */; ++i) {
			int vIX = i + foldStart;

			File validationFile = dataFiles.remove(vIX);

			if (Config.CLASSIFY_USING_SAMPLE_FIXED_NUMBER_FROM_USER) {
				validationFile = FileUtils.getFile(
						validationFile.getParentFile(), "validation",
						validationFile.getName());
			}

			validationSet[i] = new Instances(Channels.newReader(FileUtils
					.openInputStream(validationFile).getChannel(),
					Config.OUT_CHARSET));
			validationSet[i]
					.setClassIndex(validationSet[i].numAttributes() - 1);
		}

		Instances[] trainingSet = loadAugmentedInstances(dataFiles);

		result.setKey(trainingSet);
		result.setValue(validationSet);
		synchronized (foldInstanceMap) {
			foldInstanceMap.put(key, result);
		}
		return copyInsts(result);
	}

	static Instances[] loadAugmentedInstances(ArrayList<File> dataFiles)
			throws Exception {
		Instances[] trainingSet = new Instances[4];

		// train Classifier
		// boolean firstUser = true;
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

			// Instances binaryStruct = changeStruct(dataStruct);

			if (userIx == 0) {
				// if (baseClassifier instanceof UpdateableClassifier) {
				// baseClassifier.buildClassifier(dataStruct); // joinedStruct);
				// } else {
				// if (!Config.CLASSIFY_SEPARATE_PREVALENCE) {
				trainingSet[0] = new Instances(dataStruct); // joinedStruct);
				// } else {
				// trainingSet[2] = new Instances(dataStruct,0);
				// trainingSet[3] = new Instances(dataStruct,0);
				//
				// trainingSet[0] = new Instances(binaryStruct,0);
				// trainingSet[1] = new Instances(binaryStruct,0);
				// // trainingSet[0] = new Instances(binaryStruct);
				// }
			}

			// load data
			Instance dataInst;
			int instIx = 0;
			while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {

				// if (ignoreInstsWithMissingClass
				// && dataInst.classIsMissing()) {
				// continue;
				// }
				if (Config.CLASSIFY_PENALIZE_ZERO && dataInst.classValue() == 0) {
					dataInst.setWeight(0.5);
				}

				// if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
				//
				// Instance[] copies = spreadInstOnSets(dataInst, binaryStruct);
				// for (int t = 0; t < 4; ++t) {
				// if (copies[t] != null) {
				// trainingSet[0].add(copies[t]);
				// }
				// }
				//
				// } else {
				trainingSet[0].add(dataInst);
				// }
				++instIx;
			}

			++userIx;
		}
		// if (Config.CLASSIFY_SEPARATE_PREVALENCE) {
		//
		// copyInstancesIntoSets(trainingSet[0], trainingSet);
		//
		// }

		return trainingSet;
	}

	private static Instances copyInstancesIntoSets(Instances source, int l,
			boolean delete) throws Exception {
		Instances trainingSet = null;
		switch (l) {
		case 0:
			trainingSet = changeStruct(source);
			break;
		case 1:
			trainingSet = changeStruct(source);
			break;
		case 2:
			trainingSet = new Instances(source);
			break;
		case 3:
			trainingSet = new Instances(source);
			break;
		}
		for (int i = source.numInstances() - 1; i >= 0; --i) {
			Instance srcInst = (Instance) source.instance(i);
			switch ((int) srcInst.classValue()) {
			case 0:
				if (l == 0) {
					trainingSet.instance(i).setClassValue("+1");
				} else {
					if (delete) {
						trainingSet.delete(i);
					} else if (l == 1) {
						trainingSet.instance(i).setClassValue("-1");
					}
				}
				break;
			case 1:
			case 2:
			case 3:
				if (l == 0) {
					trainingSet.instance(i).setClassValue("-1");
				} else if (l == 1) {
					trainingSet.instance(i).setClassValue("+1");
				} else if (l == 3) {
					if (delete) {
						trainingSet.delete(i);
					} else {
						// nothing
					}
				}
				break;
			default:
				if (l == 0) {
					trainingSet.instance(i).setClassValue("-1");
				} else if (l == 1) {
					trainingSet.instance(i).setClassValue("-1");
				} else if (l == 2) {
					if (delete) {
						trainingSet.delete(i);
					} else {
						// nothing
					}
				}
				break;
			}
			// if (srcInst.classValue() == 0) {
			// if (l == 0) {
			// trainingSet.instance(i).setClassValue("+1");
			// } else if (l == 1) {
			// trainingSet.instance(i).setClassValue("-1");
			// } else {
			// if (delete) {
			// trainingSet.delete(i);
			// }
			//
			// }
			// } else {
			// if (l == 0) {
			// trainingSet.instance(i).setClassValue("-1");
			// } else {
			// if (srcInst.classValue() < 4) {
			// if (l == 1) {
			// trainingSet.instance(i).setClassValue("+1");
			// } else if (l == 3) {
			// if (delete) {
			// trainingSet.delete(i);
			// } else {
			// // nothing
			// }
			// }
			// } else {
			// if (l == 1) {
			// trainingSet.instance(i).setClassValue("-1");
			// } else if (l == 2) {
			// if (delete) {
			// trainingSet.delete(i);
			// } else {
			// // nothing
			// }
			// }
			// }
			// }
			// }
		}
		return trainingSet;
	}

	// private static Instance[] spreadInstOnSets(Instance dataInst, Instances
	// binaryStruct) {
	// Instance[] result = new Instance[4];
	// // Instance copy = new Instance(binaryStruct.numAttributes());
	// // cloneInstanceAttrs(dataInst,copy);
	// Instance copy = new Instance(dataInst.weight(),
	// dataInst.toDoubleArray());
	// copy.deleteAttributeAt(copy.numAttributes()-1);
	// copy.insertAttributeAt(copy.numAttributes());
	// copy.setDataset(binaryStruct);
	// if (dataInst.classValue() == 0) {
	// copy.setClassValue("+1");
	// } else {
	// copy.setClassValue("-1");
	// }
	// result[0] = (copy);
	//
	// if (dataInst.classValue() != 0) {
	//
	// if (dataInst.classValue() < 4) {
	// result[2] = (dataInst);
	// } else {
	// result[3] = (dataInst);
	// }
	//
	// // Instance cop2 = (Instance)copy.copy();
	// Instance cop2 = new Instance(binaryStruct.numAttributes());
	// cloneInstanceAttrs(dataInst,cop2);
	//
	// cop2.setDataset(binaryStruct);
	//
	// if (dataInst.classValue() < 4) {
	// cop2.setClassValue("+1");
	// } else {
	// cop2.setClassValue("-1");
	// }
	// result[1] = (cop2);
	// }
	// return result;
	// }
	//
	// private static void cloneInstanceAttrs(Instance dataInst, Instance copy)
	// {
	// for(int a = 0; a< dataInst.numAttributes()-1; ++a){
	// copy.setValue(a, dataInst.value(a));
	// }
	// copy.setWeight(dataInst.weight());
	// }

	private static Instances changeStruct(Instances dataStruct)
			throws Exception {
		Instances copyInsts = new Instances(dataStruct); // , 0);
		Add add = new Add();
		add.setAttributeIndex("last");
		add.setAttributeName("binary-label");
		add.setNominalLabels(Config.LABELS_BINARY[0] + ","
				+ Config.LABELS_BINARY[1]);
		add.setInputFormat(copyInsts);

		copyInsts = Filter.useFilter(copyInsts, add);

		// Remove the multinomial class
		Remove rem = new Remove();
		// The index range starts from 1 here
		rem.setAttributeIndices(Integer.toString(copyInsts.numAttributes() - 1));
		rem.setInputFormat(copyInsts);
		copyInsts = Filter.useFilter(copyInsts, rem);

		copyInsts.setClassIndex(copyInsts.numAttributes() - 1);

		copyInsts.setRelationName(dataStruct.relationName());

		return copyInsts;
	}

	static Map<String, Integer> foldFinishedCounts = Collections
			.synchronizedMap(new HashMap<String, Integer>());

	static synchronized void foldFinished(String positiveClass, int foldStart) {
		String key = positiveClass + foldStart;
		// Integer cnt = foldFinishedCounts.get(key);
		// if (cnt == null) {
		// cnt = 0;
		// }
		// ++cnt;
		// if (cnt == NUMBER_OF_ALGORITHMS) {
		synchronized (foldInstanceMap) {
			foldInstanceMap.remove(key);
		}
		// }
		// foldFinishedCounts.put(key, cnt);

	}

	static KeyValuePair<Instances[], Instances[]> copyInsts(
			KeyValuePair<Instances[], Instances[]> orig) {
		return orig;
		// KeyValuePair<Instances, Instances[]> result = new
		// KeyValuePair<Instances, Instances[]>();
		// result.setKey(copyInstsInternal(orig.getKey()));
		// result.setValue(new Instances[orig.getValue().length]);
		// for (int i = 0; i < orig.getValue().length; ++i) {
		// result.getValue()[i] = copyInstsInternal(orig.getValue()[i]);
		// }
		// return result;
	}

	static Instances copyInstsInternal(Instances orig) {

		Instances result = new Instances(orig);
		Enumeration instEnum = orig.enumerateInstances();
		while (instEnum.hasMoreElements()) {
			Instance inst = (Instance) instEnum.nextElement();
			result.add((Instance) inst.copy());
		}
		return result;
	}
}
