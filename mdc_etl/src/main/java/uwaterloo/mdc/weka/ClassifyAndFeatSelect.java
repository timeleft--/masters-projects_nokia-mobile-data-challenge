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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
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
import uwaterloo.mdc.etl.mallet.ImportIntoMallet;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.GainRatioAttributeEval;
import weka.attributeSelection.GreedyStepwise;
import weka.attributeSelection.Ranker;
import weka.attributeSelection.SVMAttributeEval;
import weka.attributeSelection.SubsetEvaluator;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;

public class ClassifyAndFeatSelect implements Callable<Void> {
	// Commented out are Dimensionality reductions that failed
	// Class<? extends ASEvaluation>[] cannot create!
	@SuppressWarnings("rawtypes")
	public static Class[] attrSelectEvaluationClazzes = {
	// CfsSubsetEval.class,
	// // ChiSquaredAttributeEval.class,
	// ConsistencySubsetEval.class,
	// // FilteredAttributeEval.class, FilteredSubsetEval.class,
	// GainRatioAttributeEval.class,
	// // InfoGainAttributeEval.class,
	// ReliefFAttributeEval.class,
	SVMAttributeEval.class,
	// SymmetricalUncertAttributeEval.class,
	// WrapperSubsetEval.class,

	};
	// // Attribute Transformers
	// public static Class[] attrTransEvaluationClazzes = {
	// // PrincipalComponents and
	// LatentSemanticAnalysis.class, };
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

		public void correlation(Instances trainInstances) throws IOException {
			Writer corrWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_feat-correlation.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {
				int numAttribs = trainInstances.numAttributes();
				int numInstances = trainInstances.numInstances();
				// double[][] correlation = new double[numAttribs][numAttribs];
				double[] att1 = new double[numInstances];
				double[] att2 = new double[numInstances];

				corrWr.append("\"attrib\"");
				for (int j = 0; j < numAttribs; ++j) {
					corrWr.append('\t').append(
							StringUtils.quote(trainInstances.attribute(j)
									.name()));
				}
				corrWr.append('\n');

				double corr;
				for (int i = 0; i < numAttribs; i++) {
					corrWr.append(StringUtils.quote(trainInstances.attribute(i)
							.name()));
					for (int j = 0; j < i; j++) {
						int n = 0;
						for (int k = 0; k < numInstances; k++) {
							double temp1 = trainInstances.instance(k).value(i);
							double temp2 = trainInstances.instance(k).value(j);
							if (Double.isNaN(temp1) || Double.isNaN(temp2)) {
								continue;
							}
							att1[n] = temp1;
							att2[n] = temp2;
							++n;
						}
						if (i == j) {
							// correlation[i][j] = 1.0;
							// // store the standard deviation
							// stdDevs[i] = Math.sqrt(Utils.variance(att1));
							corr = 1.0;
						} else {
							if (n <= 1) {
								corr = Double.NaN;
							} else {
								corr = Utils.correlation(att1, att2, n);
								// corr = new
								// PearsonsCorrelation().correlation(att1,
								// att2);
							}
						}
						corrWr.append('\t').append(Double.toString(corr));

						synchronized (correlationSummary[i][j]) {
							correlationSummary[i][j].addValue(corr);
						}

					}
					corrWr.append('\n');
				}
			} finally {
				corrWr.flush();
				corrWr.close();
			}
		}

		public void mutualInfo(Instances trainInstances) throws IOException {
			Writer infoWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), "v" + v
									+ "_feat-mutual-info.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {
				int numAttribs = trainInstances.numAttributes();
				int numInstances = trainInstances.numInstances();
				// double[][] infoelation = new double[numAttribs][numAttribs];
				double[] att1 = new double[numInstances];
				double[] att2 = new double[numInstances];

				infoWr.append("\"attrib\"");
				for (int j = 0; j < numAttribs; ++j) {
					infoWr.append('\t').append(
							StringUtils.quote(trainInstances.attribute(j)
									.name()));
				}
				infoWr.append('\n');

				double info;
				for (int i = 0; i < numAttribs; i++) {
					infoWr.append(StringUtils.quote(trainInstances.attribute(i)
							.name()));
					for (int j = 0; j < i; j++) {
						int n = 0;
						for (int k = 0; k < numInstances; k++) {
							double temp1 = trainInstances.instance(k).value(i);
							double temp2 = trainInstances.instance(k).value(j);
							// if (Double.isNaN(temp1) || Double.isNaN(temp2)) {
							// continue;
							// }
							att1[n] = temp1;
							att2[n] = temp2;
							++n;
						}
						if (i == j) {
							// Should never happen

							info = Double.POSITIVE_INFINITY; // 1.0;
						} else {
							if (n <= 1) {
								info = Double.NaN;
							} else {
								Frequency xFreq = new Frequency();
								Frequency yFreq = new Frequency();
								Frequency xyFreq = new Frequency();
								for (int i1 = 0; i1 < n; ++i1) {
									xFreq.addValue(att1[i1]);
									yFreq.addValue(att2[i1]);

									for (int i2 = 0; i2 < n; ++i2) {
										String xyVal = Double
												.toString(att1[i1])
												+ ","
												+ Double.toString(att2[i2]);
										xyFreq.addValue(xyVal);
									}
								}

								info = 0;
								Iterator<Comparable<?>> xIter = xFreq
										.valuesIterator();
								while (xIter.hasNext()) {
									Comparable<?> x = xIter.next();
									Iterator<Comparable<?>> yIter = yFreq
											.valuesIterator();
									while (yIter.hasNext()) {
										Comparable<?> y = yIter.next();
										String xyVal = x.toString() + ","
												+ y.toString();
										double term = xyFreq.getPct(xyVal)
												/ xFreq.getPct(x)
												* yFreq.getPct(y);
										term = MathUtil.lg2(term);
										term = xyFreq.getPct(xyVal) * term;
										info += term;
									}
								}

							}
						}
						infoWr.append('\t').append(Double.toString(info));
					}
					infoWr.append('\n');
				}
			} finally {
				infoWr.flush();
				infoWr.close();
			}
		}

		@Override
		public HashMap<String, Double> call() throws Exception {
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

				Collection<File> inputArrfs = FileUtils.listFiles(
						FileUtils.getFile(inPath), new String[] { "arff" },
						true);

				// train Classifier
				boolean firstUser = true;
				int userIx = 0;
				for (File userData : inputArrfs) {
					if (userIx == Config.NUM_USERS_TO_PROCESS) {
						break;
					}
					// if (userData.getName().startsWith("113")) {
					// continue; // too mcuh data, and might make us run out of
					// // memory
					// }

					File appData = FileUtils.getFile(FilenameUtils
							.removeExtension(userData.getAbsolutePath())
							+ ".app");

					if (userIx == (foldStart + inFoldTestIx)) {
						validationSet = new Instances(Channels.newReader(
								FileUtils.openInputStream(userData)
										.getChannel(), Config.OUT_CHARSET));
						Reader appReader = Channels.newReader(FileUtils
								.openInputStream(appData).getChannel(),
								Config.OUT_CHARSET);
						validationSet = Instances.mergeInstances(new Instances(
								appReader), validationSet);
						validationSet.setClassIndex(validationSet
								.numAttributes() - 1);
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
						while ((dataInst = dataLoader
								.getNextInstance(dataStruct)) != null) {
							appInst = appLoader.getNextInstance(appStruct);

							if (appInst == null) {
								throw new Exception(
										"App Insances fewer than data instances: "
												+ instIx);
							}
							// isClassMissing but without haveing to set the
							// clas
							if (dataInst
									.isMissing(dataInst.numAttributes() - 1)) {
								continue;
							}

							Instance joinedInst = dataInst
									.mergeInstance(appInst);
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

					// System.out.println(baseClassifierClazz.getSimpleName() +
					// " - "
					// + (System.currentTimeMillis() - startTime) + " (fold "
					// + v + "): Done reading user: " + userData.getName());
					++userIx;
				}

				if (baseClassifier instanceof UpdateableClassifier) {
					// already trained
				} else {
					if (Config.CALSSIFYFEATSELECT_CALC_CORRELATION) {
						if (correlationSummary == null) {
							correlationSummary = new SummaryStatistics[trainingSet
									.numAttributes()][];
							for (int i = 0; i < trainingSet.numAttributes(); ++i) {
								correlationSummary[i] = new SummaryStatistics[i];
								for (int j = 0; j < i; ++j) {
									correlationSummary[i][j] = new SummaryStatistics();
								}
							}
						}
						correlation(trainingSet);
					}
					if (Config.CALSSIFYFEATSELECT_CALC_MUTUALINFO) {
						mutualInfo(trainingSet);
					}
					baseClassifier.buildClassifier(trainingSet);
					trainingSet = null;
				}

				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished training for fold: " + v);
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
																		+ "_classifications.txt"))
										.getChannel(), Config.OUT_CHARSET);

				try {
					classificationsWr
							.append("instance\tclass1Prob\tclass2Prob\tclass3Prob\tclass4Prob\tclass5Prob\tclass6Prob\tclass7Prob\tclass8Prob\tclass9Prob\tclass10Prob\n");

					// TODONOT: user 113
					if (validationSet.numInstances() == 0) {
						classificationsWr
								.append("No validation data for fold: " + v);

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
							classificationsWr.append(vInst.dataset()
									.relationName() + "[" + i + "]");
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
							long trueLabelCfMIx = Math
									.round(vInst.classValue());
							long bestLabelInt = Math.round(vClass);
							foldFeactSelectCM[(int) trueLabelCfMIx]
									.addValue(bestLabelInt);
							synchronized (totalConfusionMatrix) {
								totalConfusionMatrix[(int) trueLabelCfMIx]
										.addValue(bestLabelInt);
							}
						}
						accuracyMap.put(ALL_FEATS, trueClassificationsCount
								* 1.0 / validationSet.numInstances());
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
										+ "_confusion-matrix.txt"))
								.getChannel(), Config.OUT_CHARSET);
				try {
					writeConfusionMatrix(foldConfusionWr, foldFeactSelectCM);
				} finally {
					foldConfusionWr.flush();
					foldConfusionWr.close();
				}
				System.out.println(baseClassifierClazz.getSimpleName() + " - "
						+ (System.currentTimeMillis() - startTime) + " (fold "
						+ v + "): Finished validation for fold: " + v);

				// //////////////////////////////////////

				for (@SuppressWarnings("rawtypes")
				Class attrSelectEvalClazz : attrSelectEvaluationClazzes) {

					@SuppressWarnings("unchecked")
					ASEvaluation eval = (ASEvaluation) attrSelectEvalClazz
							.getConstructor().newInstance();

					if (eval instanceof GainRatioAttributeEval) {
						((GainRatioAttributeEval) eval).setMissingMerge(false);
					}

					ASSearch search;
					if (eval instanceof SubsetEvaluator) {
						search = new GreedyStepwise();
						// ((GreedyStepwise) search).setSearchBackwards(true);
						// ((GreedyStepwise)
						// search).setNumToSelect(this.baseClassifier.)
						((GreedyStepwise) search).setGenerateRanking(true);
					} else {
						search = new Ranker();
					}

					if (eval instanceof SVMAttributeEval) {

						Add add = new Add();
						add.setAttributeIndex("last");
						add.setAttributeName("binary-label");
						add.setNominalLabels("+1,-1");
						add.setInputFormat(validationSet);

						for (String positiveClass : Config.LABEL_HIERARCHY) { // classes

							Instances copyValidation = new Instances(
									validationSet);
							
							// Should have done this earlier actually
							copyValidation.deleteWithMissingClass();

							copyValidation = Filter.useFilter(copyValidation,
									add);

							@SuppressWarnings("rawtypes")
							Enumeration instEnum = copyValidation
									.enumerateInstances();
							int positiveExamples = 0;
							while (instEnum.hasMoreElements()) {
								Instance copyInst = (Instance) instEnum
										.nextElement();

								String cls = Long.toString(Math.round(copyInst
										.classValue()) + 1);

								String binaryLabel = null;
								if (positiveClass.contains("+" + cls + "+")) {
									binaryLabel = "+1";
									++positiveExamples;

								} else if (positiveClass.contains("-" + cls
										+ "-")) {
									binaryLabel = "-1";
								}

								if (binaryLabel != null) {
									copyInst.setValue(
											copyInst.numAttributes() - 1,
											binaryLabel);
								} else {
									copyInst.setMissing(copyInst
											.numAttributes() - 1);
								}

								copyInst.setDataset(copyValidation);

							}

							Remove rem = new Remove();
							// The index range starts from 1 when it is text
							rem.setAttributeIndices(Integer
									.toString(copyValidation.numAttributes() - 1));
							rem.setInputFormat(copyValidation);
							copyValidation = Filter.useFilter(copyValidation,
									rem);

							copyValidation.setClassIndex(copyValidation
									.numAttributes() - 1);
							copyValidation.deleteWithMissingClass();
							
							copyValidation.setRelationName(validationSet
									.getRevision() + positiveClass);

							if (positiveExamples > 0) {
								HashMap<String, Double> tempAccuracy = new HashMap<String, Double>();
								Frequency[] tempCM;
								tempCM = new Frequency[Config.LABEL_HIERARCHY.length];
								for (int c = 0; c < tempCM.length; ++c) {
									tempCM[c] = new Frequency();
								}
								featSel(copyValidation, tempCM, eval, search,
										tempAccuracy,"v" + v + "_" + positiveClass);
								for (String accuKeys : tempAccuracy.keySet()) {
									accuracyMap.put(accuKeys + positiveClass,
											tempAccuracy.get(accuKeys));
								}
								// The confusion matrix needs some tweeking,
								// lest it throws exceptions (Null or o bound)
								// Writer tempCmWr = Channels
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
								// + "svm"
								// + positiveClass
								// + "_confusion-matrix.txt"))
								// .getChannel(),
								// Config.OUT_CHARSET);
								// try {
								// writeConfusionMatrix(tempCmWr, tempCM);
								// } finally {
								// tempCmWr.flush();
								// tempCmWr.close();
								// }
							}
						}
					} else {
						featSel(validationSet, foldFeactSelectCM, eval, search,
								accuracyMap,"v" + v );
					}
				}
				// // //////////////////////////////////////
				// // //////////////////////////////////////
				//
				// for (@SuppressWarnings("rawtypes")
				// Class attrTransEvalClazz : attrTransEvaluationClazzes) {
				//
				// @SuppressWarnings("unchecked")
				// ASEvaluation eval = (ASEvaluation) attrTransEvalClazz
				// .getConstructor().newInstance();
				//
				// ASSearch search;
				// search = new Ranker();
				//
				// featSel(validationSet, foldFeactSelectCM, eval, search,
				// accuracyMap);
				//
				// }
				// //////////////////////////////////////

			} catch (Exception ignored) {
				ignored.printStackTrace(System.err);
			}
			return accuracyMap;
		}

		void featSel(Instances validationSet, Frequency[] foldFeactSelectCM,
				ASEvaluation eval, ASSearch search,
				HashMap<String, Double> accuracyMap, String filenamePfx) throws IOException {
			AttributeSelectedClassifier featSelector = new AttributeSelectedClassifier();
			featSelector.setClassifier(baseClassifier);
			featSelector.setEvaluator(eval);
			featSelector.setSearch(search);

			Writer featSelectWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, baseClassifier
									.getClass().getName(), eval.getClass() // attrSelectEvalClazz
									.getName(), filenamePfx
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
							totalFeatSelectCM.get(eval.getClass() // attrSelectEvalClazz
									.getName() /*
												 * + searchClazz . getName
												 */)[(int) trueLabelCfMIx]
									.addValue(bestLabelInt);
						}
					}
					// a map of accuracies for different algot
					accuracyMap.put(eval.getClass() // attrSelectEvalClazz
							.getName(), featSelectCorrectCount * 1.0
							/ validationSet.numInstances());
					synchronized (cvFeatSelectAccuracyWr) {

						cvFeatSelectAccuracyWr
								.get(eval.getClass() // attrSelectEvalClazz
										.getName() /*
													 * + searchClazz . getName
													 */)
								.append(Integer.toString(v))
								.append('\t')
								.append(Integer
										.toString(featSelectCorrectCount))
								.append('\t')
								.append(Integer.toString(validationSet
										.numInstances()))
								.append('\t')
								.append(Double.toString(accuracyMap.get(eval
										.getClass() // attrSelectEvalClazz
										.getName()))).append('\n');
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
		}
	}

	// juust one for all threads
	static SummaryStatistics[][] correlationSummary;

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

		totalConfusionMatrix = new Frequency[Config.LABELS_SINGLES.length];
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
			Frequency[] freqArr = new Frequency[Config.LABELS_SINGLES.length];
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
		Config.quantizedFields = new Properties();
		Config.quantizedFields.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));

		CalcCutPoints.main(args);
		ImportIntoMallet.main(args);
		//
		// // Still cannot handle quantized vals
		// // CountConditionalFreqs countCond = new CountConditionalFreqs();
		// // ExecutorService countExec = Executors.newSingleThreadExecutor();
		// // countExec.submit(countCond);
		//
		// LoadCountsAsAttributes.main(args);

		ClassifyAndFeatSelect app;

		// // Naive Bayes
		// app = new ClassifyAndFeatSelect(NaiveBayesUpdateable.class);
		// app.call();

		// C4.5 decision tree
		app = new ClassifyAndFeatSelect(J48.class);
		app.call();

		// // Bayes Net
		// app = new ClassifyAndFeatSelect(BayesNet.class);
		// app.call();

		// Exception: weka.classifiers.functions.Logistic: Not enough training
		// instances with class labels (required: 1, provided: 0)!
		// Logistic Regression
		// app = new ClassifyAndFeatSelect(Logistic.class);
		// app.call();

		// Exception: weka.classifiers.functions.Logistic: Not enough training
		// // SVM
		// app = new ClassifyAndFeatSelect(LibSVM.class);
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
		SummaryStatistics accuracySummaryAllFeatures = new SummaryStatistics();
		HashMap<String, SummaryStatistics> accuracySummaryFeatSelected = new HashMap<String, SummaryStatistics>();
		try {
			for (@SuppressWarnings("rawtypes")
			Class clazz : attrSelectEvaluationClazzes) {
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
				if (accuracies == null) {
					System.err
							.println("ERROR: Null accuracies map returned by fold: "
									+ i
									+ " for classifier "
									+ this.baseClassifierClazz.getName());
					continue;
				}

				Double accu = accuracies.get(ALL_FEATS);
				if (accu == null) {
					System.err
							.println("ERROR: Null accuracy for feat selector "
									+ ALL_FEATS + " for classifier "
									+ this.baseClassifierClazz.getName());
				} else {
					accuracySummaryAllFeatures.addValue(accu);
				}

				for (@SuppressWarnings("rawtypes")
				Class clazz : attrSelectEvaluationClazzes) {
					accu = accuracies.get(clazz.getName());
					if (accu == null) {
						System.err
								.println("ERROR: Null accuracy for feat selector "
										+ clazz.getName()
										+ " for classifier "
										+ this.baseClassifierClazz.getName());
					} else {
						accuracySummaryFeatSelected.get(clazz.getName())
								.addValue(accu);
					}
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

		for (@SuppressWarnings("rawtypes")
		Class clazz : attrSelectEvaluationClazzes) {
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

		if (Config.CALSSIFYFEATSELECT_CALC_CORRELATION
				&& correlationSummary != null) {
			Writer corrWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath,
									baseClassifierClazz.getName(),
									"feat-correlation_summary.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				for (int i = 0; i < correlationSummary.length; ++i) {
					for (int j = 0; j < correlationSummary[i].length; ++j) {
						corrWr.append(
								Double.toString(correlationSummary[i][j]
										.getGeometricMean())).append('\t');
					}
					corrWr.append('\n');
				}
			} finally {
				corrWr.flush();
				corrWr.close();
			}
		}

		System.out.println(baseClassifierClazz.getSimpleName() + " - "
				+ new Date().toString() + " (total): Done in "
				+ (System.currentTimeMillis() - startTime) + " millis");
		return null;
	}

	public static void writeConfusionMatrix(Writer foldConfusionWr,
			Frequency[] foldConfusionMatrix) throws IOException {
		foldConfusionWr.append("label\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ttotal\n");
		for (int i = 0; i < foldConfusionMatrix.length; ++i) {
			foldConfusionWr.append(Integer.toString(i + 1));
			long totalCount = 0;
			for (int j = 1; j <= Config.LABELS_SINGLES.length; ++j) {
				long cnt = foldConfusionMatrix[i].getCount(j);
				totalCount += cnt;
				foldConfusionWr.append('\t').append(Long.toString(cnt));
			}
			foldConfusionWr.append('\t').append(Long.toString(totalCount))
					.append('\n');
		}
	}

}
