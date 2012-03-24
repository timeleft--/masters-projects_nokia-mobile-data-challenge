package uwaterloo.mdc.stats;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
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
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;
import weka.attributeSelection.ASEvaluation;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.GainRatioAttributeEval;
import weka.attributeSelection.GreedyStepwise;
import weka.attributeSelection.Ranker;
import weka.attributeSelection.SubsetEvaluator;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SelectedTag;
import weka.core.Utils;
import weka.core.WekaException;
import weka.core.converters.ArffLoader;

public class CalcAttrPairWiseProperty implements Callable<Void> {

	static SummaryStatistics[][] correlationSummary;

	private class FoldCallable implements Callable<Void> {

		final int v;

		// It is a summary across all folds

		public FoldCallable(int fold) {
			v = fold;
		}

		public void correlation(Instances trainInstances) throws IOException {
			Writer corrWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v
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

		public void mutualInfo(Instances trainInstances) throws Exception {
			Writer infoWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v
									+ "_feat-mutual-info.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {
				int numAttribs = trainInstances.numAttributes();
				int numInstances = trainInstances.numInstances();
				// double[][] infoelation = new double[numAttribs][numAttribs];
				double[] att1 = new double[numInstances];
				double[] att2 = new double[numInstances];

				// Header
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
					// Load all values
					for (int j = 0; j < i; j++) {
						int n = 0;
						for (int k = 0; k < numInstances; k++) {
							att1[n] = trainInstances.instance(k).value(i);
							att2[n] = trainInstances.instance(k).value(j);
							// Do we?
							// // we want to calculate base only one rows
							// // where both values exist
							// if(Double.isNaN(att1[n]) ||
							// Double.isNaN(att2[n])){
							// continue;
							// }
							++n;
						}
						if (i == j) {
							// Should never happen
							throw new Exception(
									"i==j and you are still looping");
							// info = Double.POSITIVE_INFINITY; // 1.0;
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
										String xyVal = ((Double)x).toString() + ","
												+ ((Double)y).toString();
										double term = xyFreq.getPct(xyVal)
												/ (xFreq.getPct(x) * yFreq
														.getPct(y));
										if(term == 0){
											continue;
										}
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
		public Void call() throws Exception {
			HashMap<String, Double> accuracyMap = new HashMap<String, Double>();

			try {

				int foldStart = v * Config.VALIDATION_FOLD_WIDTH;

				Instances trainingSet = null;

				// train Classifier
				boolean firstUser = true;

				int userIx = 0;
				for (File userData : allDir.listFiles(new FilenameFilter() {

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

					if (firstUser) {
						trainingSet = new Instances(dataStruct); // joinedStruct);
					}

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

						if (dataInst.classIsMissing()) {
							// .isMissing(dataInst.numAttributes() -
							// 1))
							// {
							continue;
						}

						// Instance joinedInst = dataInst
						// .mergeInstance(appInst);
						// joinedInst.setDataset(joinedStruct);

						trainingSet.add(dataInst); // joinedInst);
						// ++instIx;
					}
					// if (appLoader.getNextInstance(appStruct) !=
					// null)
					// {
					// throw new Exception(
					// "App Insances more than data instances: "
					// + instIx);
					// }

					// System.out.println(baseClassifierClazz.getSimpleName()
					// +
					// " - "
					// + (System.currentTimeMillis() - startTime) +
					// " (fold "
					// + v + "): Done reading user: " +
					// userData.getName());
					++userIx;
				}

				if (Config.CALC_ATTR_PAIRSWISE_CORRELATION) {
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
					return null;
				} else if (Config.CALC_ATTR_PAIRSWISE_MUTUALINFO) {
					mutualInfo(trainingSet);
					System.out
							.println(+(System.currentTimeMillis() - startTime)
									+ " (fold "
									+ v
									+ "): Calculated info gain matrix for fold : "
									+ v);
					return null;
				}

			} catch (Exception ignored) {
				ignored.printStackTrace(System.err);
			}
			return null;
		}
	}

	private String outputPath = "C:\\mdc-datasets\\weka\\pairwise-property";
	private String inPath = "C:\\mdc-datasets\\weka\\segmented_user";

	private File allDir = FileUtils.getFile(inPath, Config.LABELS_MULTICLASS_NAME);

	private long startTime = System.currentTimeMillis();

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

		CalcAttrPairWiseProperty app;

		app = new CalcAttrPairWiseProperty();
		app.call();
	}

	public Void call() throws Exception {
		ExecutorService foldExecutors = Executors.newSingleThreadExecutor();
		ArrayList<Future<Void>> foldFutures = new ArrayList<Future<Void>>();
		int numClassifyTasks = 0;

		for (int v = 0; v < 1/* This has not meaning! Config.VALIDATION_FOLDS */; ++v) {
			FoldCallable FoldCallable = new FoldCallable(v);
			foldFutures.add(foldExecutors.submit(FoldCallable));
			++numClassifyTasks;
		}
		for (int i = 0; i < numClassifyTasks; ++i) {
			foldFutures.get(i).get();
		}
		foldExecutors.shutdown();
		while (!foldExecutors.isTerminated()) {
			System.out.println((System.currentTimeMillis() - startTime)
					+ " (total): shutting down");
			Thread.sleep(1000);
		}
		if (Config.CALC_ATTR_PAIRSWISE_CORRELATION
				&& correlationSummary != null) {
			Writer corrWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath,
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

		System.out.println(new Date().toString() + " (total): Done in "
				+ (System.currentTimeMillis() - startTime) + " millis");
		return null;
	}

}
