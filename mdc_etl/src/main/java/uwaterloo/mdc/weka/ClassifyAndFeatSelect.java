package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
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
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.AttributeTransformer;
import weka.attributeSelection.GainRatioAttributeEval;
import weka.attributeSelection.GreedyStepwise;
import weka.attributeSelection.LatentSemanticAnalysis;
import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.attributeSelection.SVMAttributeEval;
import weka.attributeSelection.SubsetEvaluator;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.meta.AdaBoostM1;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.meta.MultiBoostAB;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SelectedTag;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;

public class ClassifyAndFeatSelect implements Callable<Void> {
	// // Commented out are Dimensionality reductions that failed
	// // Class<? extends ASEvaluation>[] cannot create!
	// @SuppressWarnings("rawtypes")
	// public static Class[] attrSelectEvaluationClazzes = {
	// // CfsSubsetEval.class,
	// // // ChiSquaredAttributeEval.class,
	// // ConsistencySubsetEval.class,
	// // // FilteredAttributeEval.class, FilteredSubsetEval.class,
	// // GainRatioAttributeEval.class,
	// // // InfoGainAttributeEval.class,
	// // ReliefFAttributeEval.class,
	// // SVMAttributeEval.class,
	// // SymmetricalUncertAttributeEval.class,
	// // WrapperSubsetEval.class,
	//
	// };
	// // // Attribute Transformers
	// // public static Class[] attrTransEvaluationClazzes = {
	// // // PrincipalComponents and
	// // LatentSemanticAnalysis.class, };
	public static final String ALL_FEATS = "all-features";
	private static final String INTERNAL_DIR_PFX = "internal-node_";

	private class FoldCallable implements
			Callable<KeyValuePair<String, HashMap<String, Double>>> {

		final Pattern positiveSplit = Pattern.compile("\\+");
		final Pattern minusSplit = Pattern.compile("\\-");
		final Classifier baseClassifier;

		final int v;

		private final File positiveClassDir;
		private String positiveClass;
		private String[] pSplits;
		private String[] mSplits;
		private boolean classifyingBinary;

		public FoldCallable(File positiveClassDir, int fold)
				throws InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException,
				NoSuchMethodException, SecurityException {
			this.positiveClassDir = positiveClassDir;
			positiveClass = positiveClassDir.getName();
			pSplits = positiveSplit.split(positiveClass);
			mSplits = minusSplit.split(positiveClass);
			this.v = fold;
			baseClassifier = baseClassifierClazz.getConstructor().newInstance();
			if (baseClassifier instanceof LibSVM) {
				// baseClassifier.setDebug(false);
				((LibSVM) baseClassifier).setKernelType(new SelectedTag(
						LibSVM.KERNELTYPE_RBF, LibSVM.TAGS_KERNELTYPE));
				// WARNING: using -h 0 may be faster
				((LibSVM) baseClassifier).setShrinking(false);
			}

			classifyingBinary = classifyBinaryHierarchy
					&& !Config.LABELS_MULTICLASS_NAME.equals(positiveClass);
		}

		@Override
		public KeyValuePair<String, HashMap<String, Double>> call()
				throws Exception {
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

				int foldStart = v * Config.VALIDATION_FOLD_WIDTH;

				Instances validationSet = null;
				Instances trainingSet = null;

				// train Classifier
//				boolean firstUser = true;
				int userIx = 0;
				for (File userData : positiveClassDir
						.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File dir, String name) {

								return name.endsWith(".arff");
							}
						})) {
					if (userIx == Config.NUM_USERS_TO_PROCESS) {
						break;
					}

					// File appData = FileUtils.getFile(FilenameUtils
					// .removeExtension(userData.getAbsolutePath())
					// + ".app");

					ArffLoader dataLoader = new ArffLoader();
					dataLoader.setFile(userData);

					// ArffLoader appLoader = new ArffLoader();
					// appLoader.setFile(appData);

					// load structure
					Instances dataStruct = dataLoader.getStructure();
					dataStruct.setClassIndex(dataStruct.numAttributes() - 1);
					// Instances appStruct =
					// appLoader.getStructure();
					// Instances joinedStruct =
					// Instances.mergeInstances(
					// appStruct, dataStruct);
					// joinedStruct
					// .setClassIndex(joinedStruct.numAttributes() -
					// 1);
					// joinedStruct.setRelationName(FilenameUtils
					// .removeExtension(userData.getName()));

					if (userIx == 0) {
						// if(firstUser
						if (baseClassifier instanceof UpdateableClassifier) {
							baseClassifier.buildClassifier(dataStruct); // joinedStruct);
						} else {
							trainingSet = new Instances(dataStruct); // joinedStruct);
						}
						validationSet = new Instances(dataStruct); // joinedStruct);
					}

					// if (userIx == (foldStart + inFoldTestIx)) {
					//
					// validationSet = new Instances(Channels.newReader(
					// FileUtils.openInputStream(userData)
					// .getChannel(), Config.OUT_CHARSET));
					// // Reader appReader =
					// // Channels.newReader(FileUtils
					// // .openInputStream(appData).getChannel(),
					// // Config.OUT_CHARSET);
					// // validationSet = Instances.mergeInstances(new
					// // Instances(
					// // appReader), validationSet);
					// validationSet.setClassIndex(validationSet
					// .numAttributes() - 1);
					// // validationSet.setRelationName(FilenameUtils
					// // .removeExtension(userData.getName()));
					// } else {

					// load data
					Instance dataInst;
					// Instance appInst;
					// int instIx = 0;
					while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {
						// appInst =
						// appLoader.getNextInstance(appStruct);
						// if (appInst == null) {
						// throw new Exception(
						// "App Insances fewer than data instances: "
						// + instIx);
						// }

						if (ignoreInstsWithMissingClass
								&& dataInst.classIsMissing()) {
							// .isMissing(dataInst.numAttributes() -
							// 1))
							// {
							continue;
						}

						// Instance joinedInst = dataInst
						// .mergeInstance(appInst);
						// joinedInst.setDataset(joinedStruct);
						if (userIx == (foldStart + inFoldTestIx)) {
							validationSet.add(dataInst); // joinedInst);
						} else {
							if (baseClassifier instanceof UpdateableClassifier) {
								((UpdateableClassifier) baseClassifier)
										.updateClassifier(dataInst); // joinedInst);
							} else {
								trainingSet.add(dataInst); // joinedInst);
							}
							// ++instIx;
						}
						// if (appLoader.getNextInstance(appStruct) !=
						// null)
						// {
						// throw new Exception(
						// "App Insances more than data instances: "
						// + instIx);
						// }
					}

					// System.out.println(baseClassifierClazz.getSimpleName()
					// +
					// " - "
					// + (System.currentTimeMillis() - startTime) +
					// " (fold "
					// + v + "): Done reading user: " +
					// userData.getName());
					++userIx;
				}

				if (baseClassifier instanceof UpdateableClassifier) {
					// already trained
				} else {
					baseClassifier.buildClassifier(trainingSet);
					trainingSet = null;
				}

				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished training for positive class: "
						+ positiveClass);

				// ////////////////////*********************/////////////////////////

				if (validationSet == null || validationSet.numInstances() == 0) {
					System.err.println("No validation data for fold: " + v
							+ " - class: " + positiveClass);
					return new KeyValuePair<String, HashMap<String, Double>>(
							positiveClass, accuracyMap);
				}

				Properties validationActualCs = new Properties();
				if (classifyingBinary) {
					validationActualCs
							.load(Channels.newReader(
									FileUtils
											.openInputStream(
													FileUtils
															.getFile(
																	positiveClassDir,
																	validationSet
																			.relationName()
																			+ "_actual-labels.properties"))
											.getChannel(), Config.OUT_CHARSET));
				}

				Writer classificationsWr = Channels
						.newWriter(
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																outputPath,
																baseClassifier
																		.getClass()
																		.getName(),
																"v"
																		+ v
																		+ "_"
																		+ positiveClass
																		+ "_classifications.txt"))
										.getChannel(), Config.OUT_CHARSET);

				try {
					classificationsWr
							.append("instance\tclass0\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");

					// TODONOT: user 113

					int trueClassificationsCount = 0;
					int trueInternalDirectionCount = 0;
					int totalClassificationsCount = (classifyingBinary ? 0
							: validationSet.numInstances());
					int totalInternalDirections = 0;
					for (int i = 0; i < validationSet.numInstances(); ++i) {
						Instance vInst = validationSet.instance(i);

						if (ignoreInstsWithMissingClass
								&& vInst.classIsMissing()) {
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
								vClass = j;
							}
						}
						classificationsWr.append('\n');
						// The class "Value" is actually its
						// index!!!!!!
						if (vClass == vInst.classValue()) {
							// Correct.. but what
							if (classifyingBinary) {
								if (// +1 going to a leaf
								(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length == 3)
										// -1 going to a leaf
										|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length == 3)) {

									// // there is only 1 +C+
									// String actualC = validationActualCs
									// .getProperty(Long.toString(Math
									// .round(vInst.value(0))));
									// if (pSplits[1].equals(actualC)) {
									++trueClassificationsCount;

								} else if (// +1 going to a non-leaf
								(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length > 3)
										// -1 going to a non-leaf
										|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length > 3)) {

									// No meaning for accuracy at non leaves
									// ++trueClassificationsCount;
									++trueInternalDirectionCount;
								}
							} else {
								++trueClassificationsCount;
							}
						}
						if (classifyingBinary) {
							// Count number that should go to a class
							// leaf or get directed to a subtree
							if (vInst.classValue() == 0) {
								// +1
								if (pSplits.length == 3) {
									++totalClassificationsCount;
								} else {
									++totalInternalDirections;
								}
							} else {
								// -1
								if (mSplits.length == 3) {
									++totalClassificationsCount;
								} else {
									++totalInternalDirections;
								}
							}
						}

						long trueLabelCfMIx;
						if (classifyingBinary) {
							// Get the true label from the properties file,
							// Using the value of the ID attribute as key
							// ID is at index 0
							trueLabelCfMIx = Long.parseLong(validationActualCs
									.getProperty(Long.toString(Math.round(vInst
											.value(0)))));
						} else {
							trueLabelCfMIx = Math.round(vInst.classValue());
						}
						// Will not happen at all.. remember that
						// .classvalue returns the index in the nominal
						// if(Config.CLASSIFY_USING_BIANRY_ENSEMBLE){
						// if(trueLabelCfMIx == -1){
						// trueLabelCfMIx = 0;
						// }
						// }

						long bestLabelInt = Math.round(vClass);
						// foldConfusionMatrix[(int) trueLabelCfMIx]
						// .addValue(bestLabelInt);
						foldConfusionMatrix[(int) trueLabelCfMIx]
								.addValue((classifyingBinary ? vClass == (vInst
										.classValue()) : bestLabelInt));
						synchronized (totalConfusionMatrix) {
							totalConfusionMatrix.get(positiveClass)[(int) trueLabelCfMIx]
									// .addValue(bestLabelInt);
									.addValue((classifyingBinary ? vClass == (vInst
											.classValue()) : bestLabelInt));
						}
					}
					if (totalClassificationsCount > 0) {
						accuracyMap.put(ALL_FEATS, trueClassificationsCount
								* 1.0 / totalClassificationsCount);

						synchronized (cvClassificationAccuracyWr) {

							cvClassificationAccuracyWr
									.get(positiveClass)
									.append(Integer.toString(v))
									.append('\t')
									.append(Integer
											.toString(trueClassificationsCount))
									.append('\t')
									.append(Integer
											.toString(totalClassificationsCount))
									.append('\t')
									.append(Double.toString(accuracyMap
											.get(ALL_FEATS))).append('\n');
						}
					}
					if (totalInternalDirections > 0) {
						accuracyMap.put(INTERNAL_DIR_PFX + ALL_FEATS,
								trueInternalDirectionCount * 1.0
										/ totalInternalDirections);

						synchronized (cvClassificationAccuracyWr) {

							cvClassificationAccuracyWr
									.get(positiveClass)
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
											.get(INTERNAL_DIR_PFX + ALL_FEATS)))
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
										.getClass().getName(), "v" + v + "_"
										+ positiveClass
										+ "_confusion-matrix.txt"))
								.getChannel(), Config.OUT_CHARSET);
				try {
					writeConfusionMatrix(foldConfusionWr, foldConfusionMatrix,
							classifyingBinary);
				} finally {
					foldConfusionWr.flush();
					foldConfusionWr.close();
				}
				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished validation for  class: "
						+ positiveClass);

				// //////////////////////////////////////
				if (featSelect) {
					for (@SuppressWarnings("rawtypes")
					Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {

						@SuppressWarnings("unchecked")
						ASEvaluation eval = (ASEvaluation) attrSelectEvalClazz
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
						featSel(validationSet, foldFeactSelectCM, eval, search,
								accuracyMap, "v" + v, validationActualCs);

					}

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

			} catch (Exception ignored) {
				ignored.printStackTrace(System.err);
			}

			return new KeyValuePair<String, HashMap<String, Double>>(
					positiveClass, accuracyMap);
		}

		void featSel(Instances validationSet, Frequency[] foldFeactSelectCM,
				ASEvaluation eval, ASSearch search,
				HashMap<String, Double> accuracyMap, String filenamePfx,
				Properties validationActualCs) throws IOException {
			AttributeSelectedClassifier featSelector = new AttributeSelectedClassifier();
			featSelector.setClassifier(baseClassifier);
			featSelector.setEvaluator(eval);
			featSelector.setSearch(search);

			Writer featSelectWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), eval.getClass() // attrSelectEvalClazz
									.getName(), filenamePfx + "_"
									+ positiveClass
									+ "_feat-selected-classifications.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				featSelector.buildClassifier(validationSet);

				featSelectWr
						.append("instance\tclass0Prob\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");
				if (validationSet.numInstances() == 0) {
					featSelectWr.append("Not validation data for fold: " + v);

				} else {
					int featSelectCorrectCount = 0;
					int featSelTotalCount = (classifyingBinary ? 0
							: validationSet.numInstances());
					int totalInternalDirections = 0;
					int trueInternalDirectionCount = 0;
					for (int i = 0; i < validationSet.numInstances(); ++i) {
						Instance vInst = validationSet.instance(i);

						if (classifyingBinary && vInst.classIsMissing()) {
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
								vClass = j;
							}
						}
						featSelectWr.append('\n');
						// // The class "Value" is actually its index!!!!!!
						// if (vClass == vInst.classValue()) {
						// ++featSelectCorrectCount;
						// }
						if (vClass == vInst.classValue()) {
							if (classifyingBinary) {
								if (// +1 going to a leaf
								(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length == 3)
										// -1 going to a leaf
										|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length == 3)) {

									// // there is only 1 +C+
									// String actualC = validationActualCs
									// .getProperty(Long.toString(Math
									// .round(vInst.value(0))));
									// if (pSplits[1].equals(actualC)) {
									++featSelectCorrectCount;

								} else if (// +1 going to a non-leaf
								(vClass == Config.LABLES_BINARY_POSITIVE_IX && pSplits.length > 3)
										// -1 going to a non-leaf
										|| (vClass == Config.LABLES_BINARY_NEGATIVE_IX && mSplits.length > 3)) {

									// No meaning for accuracy at non leaves
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
									++featSelectCorrectCount;
								} else {
									++totalInternalDirections;
								}
							} else {
								// -1
								if (mSplits.length == 3) {
									++featSelectCorrectCount;
								} else {
									++totalInternalDirections;
								}
							}
						}
						long trueLabelCfMIx;
						if (classifyingBinary) {
							// Get the true class from the properties file
							// Using the .. oh, why did I copy paste..look above
							// :(
							// 0 is the index of the ID attrib
							trueLabelCfMIx = Long.parseLong(validationActualCs
									.getProperty(Long.toString(Math.round(vInst
											.value(0)))));
						} else {
							trueLabelCfMIx = Math.round(vInst.classValue());
						}
						long bestLabelInt = Math.round(vClass);

						foldFeactSelectCM[(int) trueLabelCfMIx]
								.addValue((classifyingBinary ? vClass == (vInst
										.classValue()) : bestLabelInt));

						// foldFeactSelectCM[(int) trueLabelCfMIx]
						// .addValue(bestLabelInt);

						synchronized (totalFeatSelectCM) {
							totalFeatSelectCM.get(positiveClass).get(
									eval.getClass() // attrSelectEvalClazz
											.getName() /*
														 * + searchClazz .
														 * getName
														 */)[(int) trueLabelCfMIx]
									// .addValue(bestLabelInt);
									.addValue((classifyingBinary ? vClass == (vInst
											.classValue()) : bestLabelInt));
						}
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
									.append(Integer.toString(featSelTotalCount))
									.append('\t')
									.append(Double.toString(accuracyMap
											.get(eval.getClass() // attrSelectEvalClazz
													.getName()))).append('\n');
						}
					}
					if (totalInternalDirections > 0) {
						// a map of accuracies for different algot
						accuracyMap.put(INTERNAL_DIR_PFX + eval.getClass() // attrSelectEvalClazz
								.getName(), trueInternalDirectionCount * 1.0
								/ totalInternalDirections);
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

			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifier.getClass().getName(), eval.getClass() // attrSelectEvalClazz
							.getName(), filenamePfx + "_feat-selection.txt"),
					featSelector.toString());
			// algo name
			System.out.println(baseClassifierClazz.getSimpleName() + "/"
					+ eval.getClass() // attrSelectEvalClazz
							.getSimpleName() + " - "
					+ (System.currentTimeMillis() - startTime) + " (fold " + v
					+ "): Finished feature selection for " + filenamePfx);

			if (eval instanceof AttributeTransformer) {
				System.out.println(baseClassifierClazz.getSimpleName() + "/"
						+ eval.getClass() // attrSelectEvalClazz
								.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
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
					ignored.printStackTrace(new PrintStream(trOut));
				} finally {
					trOut.flush();
					trOut.close();
				}
			}
		}
	}

	private String outputPath = "C:\\mdc-datasets\\weka\\validation";
	private String inPath = "C:\\mdc-datasets\\weka\\segmented_user";

	private final Map<String, Frequency[]> totalConfusionMatrix;
	private final Map<String, Map<String, Frequency[]>> totalFeatSelectCM;
	private long startTime = System.currentTimeMillis();

	private final int inFoldTestIx = new Random(System.currentTimeMillis())
			.nextInt(Config.VALIDATION_FOLD_WIDTH);

	private final Map<String, Writer> cvClassificationAccuracyWr;
	private final Map<String, Map<String, Writer>> cvFeatSelectAccuracyWr;

	private Class<? extends Classifier> baseClassifierClazz;
	private final boolean ignoreInstsWithMissingClass;
	private final boolean classifyBinaryHierarchy;
	private final boolean classifyMultiClass;
	private final boolean featSelect;
	private final Class[] attrSelectEvaluationClazzes;

	private File[] classHierDirs;
	private double acrossPCsAllFeatAccuNumer;
	private double acrossPCsAllFeatAccuDenim;

	public ClassifyAndFeatSelect(
			Class<? extends Classifier> baseClassifierClazz,
			boolean pClassifyMultiClass, boolean pClassifyBinaryHierarchy,
			boolean pFeatSelect, Class[] pAttrSelectEvaluationClazzes,
			boolean pIgnoreInstsWithMissingClass) throws IOException {
		this.classifyBinaryHierarchy = pClassifyBinaryHierarchy;
		this.classifyMultiClass = pClassifyMultiClass;
		this.ignoreInstsWithMissingClass = pIgnoreInstsWithMissingClass;
		this.featSelect = pFeatSelect;
		this.attrSelectEvaluationClazzes = pAttrSelectEvaluationClazzes;

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
			for (@SuppressWarnings("rawtypes")
			Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
				Frequency[] freqArr = new Frequency[Config.LABELS_SINGLES.length];
				for (int i = 0; i < freqArr.length; ++i) {
					freqArr[i] = new Frequency();
				}
				pcTotalFeatSelectCM.put(attrSelectEvalClazz.getName() /*
																	 * +
																	 * searchClazz
																	 */,
						freqArr);

				Writer writer = Channels.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(outputPath,
										baseClassifierClazz.getName(),
										attrSelectEvalClazz.getName() /*
																	 * +
																	 * searchClazz
																	 */,
										"feat-selected-accuracy-cv.txt"))
								.getChannel(), Config.OUT_CHARSET);

				writer.append("vFold").append('\t').append("trueN")
						.append('\t').append("totalN").append('\t')
						.append("accuracy").append('\n');
				pcCvFeatSelectAccuracyWr.put(
						attrSelectEvalClazz.getName() /*
													 * + searchClazz
													 */, writer);

			}
			totalFeatSelectCM.put(positiveClass, pcTotalFeatSelectCM);
			cvFeatSelectAccuracyWr.put(positiveClass, pcCvFeatSelectAccuracyWr);
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
		Config.quantizedFields = new Properties();
		Config.quantizedFields.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));

		// CalcCutPoints.main(args);
		// ImportIntoMallet.main(args);
		// //
		// // Still cannot handle quantized vals
		// // CountConditionalFreqs countCond = new CountConditionalFreqs();
		// // ExecutorService countExec = Executors.newSingleThreadExecutor();
		// // countExec.submit(countCond);
		//
		// LoadCountsAsAttributes.main(args);

		ClassifyAndFeatSelect app;

		// C4.5 decision tree
		app = new ClassifyAndFeatSelect(
				J48.class,
				true,
				false,
				true,
				new Class[] { GainRatioAttributeEval.class,
						PrincipalComponents.class, LatentSemanticAnalysis.class },
				false);
		app.call();

		// // Naive Bayes
		// app = new ClassifyAndFeatSelect(NaiveBayesUpdateable.class);
		// app.call();

		// // Boosting
		// app = new ClassifyAndFeatSelect(
		// AdaBoostM1.class,
		// true,
		// false,
		// true,
		// new Class[] { GainRatioAttributeEval.class,
		// PrincipalComponents.class, LatentSemanticAnalysis.class },
		// false);
		// app.call();
		// app = new ClassifyAndFeatSelect(
		// MultiBoostAB.class,
		// true,
		// false,
		// true,
		// new Class[] { GainRatioAttributeEval.class,
		// PrincipalComponents.class, LatentSemanticAnalysis.class },
		// false);
		// app.call();

		// // Bayes Net
		// app = new ClassifyAndFeatSelect(BayesNet.class);
		// app.call();

		// Exception: weka.classifiers.functions.Logistic: Not enough training
		// instances with class labels (required: 1, provided: 0)!
		// Logistic Regression
		// app = new ClassifyAndFeatSelect(Logistic.class);
		// app.call();

		// // Sometimes: Exception: weka.classifiers.functions.Logistic: Not
		// enough training
		// // SVM
		// app = new ClassifyAndFeatSelect(LibSVM.class, false, true, true,
		// new Class[] { SVMAttributeEval.class }, false);
		// app.call();

		// // Cannot handle multinomial attrs
		// // Bayesian Logisitc Regression
		// app = new ClassifyAndFeatSelect(BayesianLogisticRegression.class);
		// app.call();

		// // weka.clusterers.SimpleKMeans: Cannot handle missing class values!
		// // By clustering
		// app = new ClassifyAndFeatSelect(ClassificationViaClustering.class);
		// app.call();
		//

		// countExec.shutdown();
		// while (!countExec.isTerminated()) {
		// Thread.sleep(5000);
		// }

	}

	public Void call() throws Exception {
		HashMap<String, SummaryStatistics> accuracySummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> accuracySummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();
		HashMap<String, SummaryStatistics> internalDirSummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> internalDirSummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();
		HashMap<String, SummaryStatistics> multicAccuracySummaryAllFeatures = new HashMap<String, SummaryStatistics>();
		HashMap<String, HashMap<String, SummaryStatistics>> multicAccuracySummaryFeatSelected = new HashMap<String, HashMap<String, SummaryStatistics>>();

		try {

			System.out.println(baseClassifierClazz.getSimpleName() + " - "
					+ new Date().toString()
					+ " (total): Validating with every user number "
					+ inFoldTestIx + " within every "
					+ Config.VALIDATION_FOLD_WIDTH + " users");
			ExecutorService foldExecutors = Executors
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

				for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
					FoldCallable FoldCallable = new FoldCallable(
							positiveClassDir, v);
					foldFutures.add(foldExecutors.submit(FoldCallable));
					++numClassifyTasks;
				}
			}
			for (int i = 0; i < numClassifyTasks; ++i) {
				KeyValuePair<String, HashMap<String, Double>> accuracies = foldFutures
						.get(i).get();
				if (accuracies == null) {
					System.err
							.println("ERROR: Null accuracies map returned by fold: "
									+ i
									+ " for classifier "
									+ this.baseClassifierClazz.getName());
					continue;
				}

				if (Config.LABELS_MULTICLASS_NAME.equals(accuracies.getKey())) {
					// Multiclass not part of the consensus
					gatherSummaryStats(accuracies,
							multicAccuracySummaryAllFeatures,
							multicAccuracySummaryFeatSelected, false);

				} else {
					gatherSummaryStats(accuracies, accuracySummaryAllFeatures,
							accuracySummaryFeatSelected, false);
					gatherSummaryStats(accuracies,
							internalDirSummaryAllFeatures,
							internalDirSummaryFeatSelected, true);
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
			for (Writer wr : cvClassificationAccuracyWr.values()) {
				wr.flush();
				wr.close();
			}
			for (Map<String, Writer> pcCvFeatSelectAccuracyWr : cvFeatSelectAccuracyWr
					.values()) {
				for (Writer wr : pcCvFeatSelectAccuracyWr.values()) {
					wr.flush();
					wr.close();
				}
			}
		}
		if (classifyMultiClass) {
			// There is no internal node, it's just multi-class
			printSummaryStats(Config.LABELS_MULTICLASS_NAME,
					multicAccuracySummaryAllFeatures
							.get(Config.LABELS_MULTICLASS_NAME),
					multicAccuracySummaryFeatSelected
							.get(Config.LABELS_MULTICLASS_NAME), null, null,
					false);
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

				printSummaryStats(positiveClass, pcAccuSummaryAllFeats,
						pcAccuracySummaryFeatSelected,
						pcInternalDirSummaryAllFeatures,
						pcInternalDirSummaryFeatSelected, true);
			}

			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifierClazz.getName(),
					"binary-ensemble_consensus_accuracy-summary.txt"), Double
					.toString(acrossPCsAllFeatAccuNumer
							/ acrossPCsAllFeatAccuDenim));
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
			foldConfusionWr.append("label\t0\t1t\2\t3\t4\t5\t6\t7\t8\t9\t10\n");
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
				for (int j = 0; j <= Config.LABELS_SINGLES.length; ++j) {
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
			System.err.println("ERROR: Null accuracy for feat selector "
					+ (internal ? INTERNAL_DIR_PFX : "") + ALL_FEATS
					+ " for classifier " + this.baseClassifierClazz.getName());
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

		for (@SuppressWarnings("rawtypes")
		Class clazz : attrSelectEvaluationClazzes) {
			accu = accuracies.getValue().get(
					(internal ? INTERNAL_DIR_PFX : "") + clazz.getName());
			if (accu == null) {
				System.err.println("ERROR: Null accuracy for feat selector "
						+ clazz.getName() + " for classifier "
						+ this.baseClassifierClazz.getName());
			} else {
				HashMap<String, SummaryStatistics> accSummMap = accuracySummaryFeatSelected
						.get(accuracies.getKey());
				if (accSummMap == null) {
					accSummMap = new HashMap<String, SummaryStatistics>();
					accuracySummaryFeatSelected.put(accuracies.getKey(),
							accSummMap);
				}

				SummaryStatistics accSumm = accSummMap
						.get((internal ? INTERNAL_DIR_PFX : "")
								+ clazz.getName());
				if (accSumm == null) {
					accSumm = new SummaryStatistics();
					accuracySummaryFeatSelected.get(accuracies.getKey()).put(
					// No need.. the map will be different already (internal ?
					// INTERNAL_DIR_PFX : "") +
							clazz.getName(), accSumm);
				}
				accSumm.addValue(accu);
			}
		}
	}

	void printSummaryStats(
			String positiveClass,
			SummaryStatistics pcAccuSummaryAllFeats,
			HashMap<String, SummaryStatistics> pcAccuracySummaryFeatSelected,
			SummaryStatistics pcInternalDirSummaryAllFeatures,
			HashMap<String, SummaryStatistics> pcInternalDirSummaryFeatSelected,
			boolean expectInternal) throws IOException {

		if (expectInternal) {
			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifierClazz.getName(), positiveClass
							+ "_internal-dir-summary.txt"),
					pcInternalDirSummaryAllFeatures.toString());
		}

		FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
				baseClassifierClazz.getName(), positiveClass
						+ "_accuracy-summary.txt"), pcAccuSummaryAllFeats
				.toString());

		// The Mean of Means.. the variance would be from the t distrib
		// But many accuracies have n = 0, thus we shouldn't increase denim
		// coz those means are meaningless
		if (pcAccuSummaryAllFeats.getN() > 0) {
			acrossPCsAllFeatAccuNumer += pcAccuSummaryAllFeats.getMean();
			++acrossPCsAllFeatAccuDenim;
		}

		// // Grand Mean
		// acrossPCsAllFeatAccuNumer += pcAccuSummaryAllFeats.getSum();
		// //pcAccuSummaryAllFeats.getN() * pcAccuSummaryAllFeats.getMean();
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

		for (@SuppressWarnings("rawtypes")
		Class clazz : attrSelectEvaluationClazzes) {
			FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
					baseClassifierClazz.getName(), clazz.getName(),
					positiveClass + "_feat-selected_accuracy-summary.txt"),
					pcAccuracySummaryFeatSelected.get(clazz.getName())
							.toString());
			if (expectInternal) {
				FileUtils.writeStringToFile(FileUtils.getFile(outputPath,
						baseClassifierClazz.getName(), clazz.getName(),
						positiveClass
								+ "_feat-selected_internal-dir-summary.txt"),
						pcInternalDirSummaryFeatSelected.get(clazz.getName())
								.toString());
			}
		}

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

		for (@SuppressWarnings("rawtypes")
		Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {
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
															positiveClass
																	+ "_feat-selected-confusion-matrix.txt"))
									.getChannel(), Config.OUT_CHARSET);
			try {
				writeConfusionMatrix(featSelectedConfusionWr, totalFeatSelectCM
						.get(positiveClass)
						.get(attrSelectEvalClazz.getName()/*
														 * + searchClazz .
														 * getName
														 */),
						!Config.LABELS_MULTICLASS_NAME.equals(positiveClass));
			} finally {
				featSelectedConfusionWr.flush();
				featSelectedConfusionWr.close();
			}
		}

	}

}
