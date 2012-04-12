package mdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.EuclideanDistance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;

public class Test implements Callable<Void> {

	private static final String[] LABELS = new String[] { "0", "1", "2", "3",
			"4", "5", "6", "7", "8", "9", "10" };
	public static final String[] LABELS_BINARY = new String[] { "+1", "-1" };
	public static final int LABLES_BINARY_POSITIVE_IX = 0;
	public static final int LABLES_BINARY_NEGATIVE_IX = 1;
	private static final boolean validate = true;
	private static final Properties placeLabels = new Properties();
	private static final boolean verbose = false;

	private static int numThreads;
	private static Instances trainingSet;
	private static File[] testFiles;
	private static String outPath;

	double supRatio;
	int backLength;
	Random rand = new Random(System.currentTimeMillis());

	public Test(double s, int b) {
		supRatio = s;
		backLength = b;
	}

	public Void call() throws Exception {

		Map<File, double[][]> userInstDistribs = new HashMap<File, double[][]>();
		Map<File, int[]> userHonoredClassifier = new HashMap<File, int[]>();
		Map<String, SummaryStatistics[]> placeDistribsMap = new TreeMap<String, SummaryStatistics[]>();

		for (int l = 0; l < 4; ++l) {
			Instances training = copyInstancesIntoSets(trainingSet, l, true);
			Classifier cls = new RandomForest();
			cls.buildClassifier(training);

			for (File userFile : testFiles) {

				String filenamePfx = "n" + supRatio + "_b" + backLength + "_l"
						+ l + "_ALL";

				Instances test = new Instances(Channels.newReader(FileUtils
						.openInputStream(userFile).getChannel(), "US-ASCII"));
				test.setClassIndex(test.numAttributes() - 1);
				test = copyInstancesIntoSets(test, l, false);

				double[][] instDistribs = userInstDistribs.get(userFile);
				if (instDistribs == null) {
					instDistribs = new double[test.numInstances()][LABELS.length];
					userInstDistribs.put(userFile, instDistribs);
				}

				int[] honoredClassifier = userHonoredClassifier.get(userFile);
				if (honoredClassifier == null) {
					honoredClassifier = new int[test.numInstances()];
					userHonoredClassifier.put(userFile, honoredClassifier);
				}

				EuclideanDistance distanceMeasure = new EuclideanDistance(test);
				LinkedList<Instance> withinDayInsts = new LinkedList<Instance>();

				int prevLabelStart = test.numAttributes() - LABELS.length - 1;
				double prevInstLabel = Double.NaN;

				Properties instIdPlaceId = new Properties();
				InputStream idsIs = FileUtils.openInputStream(FileUtils
						.getFile(userFile.getParentFile().getParentFile(),
								test.relationName()
										+ "_instid-placeid_map.properties"));
				instIdPlaceId.load(idsIs);
				idsIs.close();

				BufferedReader instIDVisitTimeRd = new BufferedReader(
						Channels.newReader(
								FileUtils
										.openInputStream(
												FileUtils.getFile(
														userFile.getParentFile()
																.getParentFile(),
														test.relationName()
																+ "_instid-time_map.properties"))
										.getChannel(), "US-ASCII"));
				Writer instIDVisitTimeWr = Channels
						.newWriter(
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																outPath,
																filenamePfx
																		+ "_feat-selected_instid-time_prediction.csv"))
										.getChannel(), "US-ASCII");
				try {
					instIDVisitTimeWr.append(instIDVisitTimeRd.readLine()
							+ "\tPredictedLabel\tDistribution\n");

					for (int i = 0; i < test.numInstances(); ++i) {
						Instance tInst = test.instance(i);
						for (int p = 0; p < LABELS.length; ++p) {
							tInst.setValue(prevLabelStart + p,
									((l > 1 && p == prevInstLabel) ? 1 : 0));
						}

						double[] dist = cls.distributionForInstance(tInst);

						if (l == 0) {
							instDistribs[i][0] = dist[LABLES_BINARY_POSITIVE_IX] + 1E-6;
							for (int c = 1; c < instDistribs[i].length; ++c) {
								instDistribs[i][c] = dist[LABLES_BINARY_NEGATIVE_IX] + 1E-6;
							}
						} else if (l == 1) {
							for (int c = 0; c < instDistribs[i].length; ++c) {

								int p = (c == 1 || c == 3 ? LABLES_BINARY_POSITIVE_IX
										: LABLES_BINARY_NEGATIVE_IX);

								double numerator = dist[p] * instDistribs[i][c];

								double denim = dist[p] + instDistribs[i][c];

								if (denim != 0) {
									instDistribs[i][c] = numerator / denim;
								}
							}

							double maxProb = instDistribs[i][0];
							LinkedList<Integer> honoredList = new LinkedList<Integer>();
							for (int c : Arrays.asList(0, 1, 4)) {
								if (instDistribs[i][c] >= maxProb) {
									if (instDistribs[i][c] > maxProb) {
										honoredList.clear();
									}

									honoredList.add(c);

									maxProb = instDistribs[i][c];
								}
							}
							honoredClassifier[i] = honoredList.get(rand
									.nextInt(honoredList.size()));
							if (honoredClassifier[i] == 2
									|| honoredClassifier[i] > 3) {
								honoredClassifier[i] = 3;
							} else if (honoredClassifier[i] > 0) {
								honoredClassifier[i] = 2;
							} else {
								honoredClassifier[i] = -1;
							}
						} else if (l == honoredClassifier[i]) {
							instDistribs[i] = dist;
						}

						String instId = Double.toString(1.0 * i + 1);

						String placeId = instIdPlaceId.getProperty(instId);

						if (l == 3) {
							if (honoredClassifier[i] == -1) {
								normalizeProbabilities(instDistribs[i]);
							}
							if (verbose){
								System.out.println("Replacing "
										+ Arrays.toString(dist) + " by "
										+ Arrays.toString(instDistribs[i]));
							}
							dist = instDistribs[i];

							SummaryStatistics[] placeDistribs = placeDistribsMap
									.get(placeId);
							if (placeDistribs == null) {
								placeDistribs = new SummaryStatistics[LABELS.length];
								for (int p = 0; p < placeDistribs.length; ++p) {
									placeDistribs[p] = new SummaryStatistics();
								}

								placeDistribsMap.put(placeId, placeDistribs);
							}

							for (int p = 0; p < placeDistribs.length; ++p) {
								placeDistribs[p].addValue(dist[p]);
							}
						}

						double label = getClassFromDistrib(dist, tInst,
								withinDayInsts, distanceMeasure);
						String line = instIDVisitTimeRd.readLine();
						if (!line.startsWith(test.relationName() + "\t"
								+ instId)) {
							throw new AssertionError(
									"Visit times and labels are not inline");
						}
						instIDVisitTimeWr.append(line + "\t" + label + "\t"
								+ Arrays.toString(dist) + "\n");
						prevInstLabel = label;
					}

				} finally {
					instIDVisitTimeWr.flush();
					instIDVisitTimeWr.close();

					instIDVisitTimeRd.close();
				}
			}
		}

		writePlaceSummary(placeDistribsMap, "AVG");
//		writePlaceSummary(placeDistribsMap, "MAX");

		return null;
	}

	private void writePlaceSummary(
			Map<String, SummaryStatistics[]> placeDistribsMap, String measure)
			throws IOException {

		int[][] confusion = new int[LABELS.length][LABELS.length];

		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath, "n" + supRatio + "_b"
								+ backLength + "_place-distribs-" + measure
								+ ".csv")).getChannel(), "US-ASCII");
		try {

			wr.append("user_id\tplace_id\tN\tp0\tp1\tp2\tp3\tp4\tp5\tp6\tp7\tp8\tp9\tp10\tPredictedLabel");
			if (validate) {
				wr.append("\tActualLabel");
			}
			wr.append("\n");

			for (String placeId : placeDistribsMap.keySet()) {

				String actualLabel = placeLabels.getProperty(placeId, "0");

				int underScoreIx = placeId.indexOf('_');
				String userId = placeId.substring(0, underScoreIx);
				wr.append(userId + "\t").append(
						placeId.substring(underScoreIx + 1) + "\t");

				SummaryStatistics[] placeDist = placeDistribsMap.get(placeId);
				wr.append(placeDist[0].getN() + "\t");

				double maxProb = 1E-9;
				LinkedList<Integer> result = new LinkedList<Integer>();

				for (int c = 0; c < LABELS.length; ++c) {

					double prob = ("AVG".equals(measure) ? placeDist[c]
							.getMean() : placeDist[c].getMax());

					wr.append(prob + "\t");

					double probDiv = prob / maxProb;

					if (probDiv >= 1 / supRatio) {
						if (probDiv > supRatio) {
							result.clear();
						}
						result.add(c);
						if (probDiv > 1) {
							maxProb = prob;
						}
					}
				}

				int label = result.get(rand.nextInt(result.size()));
				wr.append(Integer.toString(label));
				if (validate) {
					wr.append("\t" + actualLabel);
					++confusion[Integer.parseInt(actualLabel)][label];
				}
				wr.append("\n");
			}
		} finally {
			wr.flush();
			wr.close();
		}

		wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath, "n" + supRatio + "_b"
								+ backLength + "_evaluation_" + measure
								+ ".csv")).getChannel(), "US-ASCII");
		try {
			writeEvaluation(confusion, wr);
		} finally {
			wr.flush();
			wr.close();
		}
	}

	private double getClassFromDistrib(double[] dist, Instance tInst,
			LinkedList<Instance> withinDayInsts,
			EuclideanDistance distanceMeasure) {
		double maxProb = 1E-9;
		LinkedList<Integer> result = new LinkedList<Integer>();
		double res = Double.NaN;

		for (int j = 0; j < dist.length; ++j) {
			double probDiv = dist[j] / maxProb;
			if (probDiv >= 1 / supRatio) {
				if (probDiv > supRatio) {
					result.clear();
				}
				result.add(j);
				if (probDiv > 1) {
					maxProb = dist[j];
				}
			}
		}

		if (result.size() == 1) {
			res = result.getFirst().doubleValue();
		} else if (result.size() > 1) {

			Iterator<Instance> dayIter = withinDayInsts.iterator();
			Instance closestInst = null;
			double minD = Double.MAX_VALUE;

			while (dayIter.hasNext()) {
				Instance prevInst = dayIter.next();
				double d = distanceMeasure.distance(tInst, prevInst);

				if (d < minD) {
					closestInst = prevInst;
					minD = d;
				}
			}

			if (closestInst != null) {
				Integer prevLabel = (int) Math.round(closestInst.classValue());

				if (result.contains(prevLabel)
						&& (closestInst.weight() / maxProb > supRatio)) {
					res = prevLabel.doubleValue();
				}
			}

			if (Double.isNaN(res)) {
				res = result.get(rand.nextInt(result.size())).doubleValue();
			}
		}

		tInst.setClassValue(res);
		tInst.setWeight(maxProb);
		withinDayInsts.addFirst(tInst);
		while (withinDayInsts.size() > backLength) {
			withinDayInsts.removeLast();
		}

		return res;

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (validate) {
			InputStream pLabelsIs = FileUtils.openInputStream(FileUtils
					.getFile("C:\\mdc-datasets\\place-labels.properties"));
			placeLabels.load(pLabelsIs);
			pLabelsIs.close();
		}

		List<String> argList = Arrays.asList(args);

		int a;

		numThreads = 1;
		a = argList.indexOf("-threads");
		if (a >= 0) {
			numThreads = Integer.parseInt(argList.get(argList.indexOf(a + 1)));
		}

		outPath = "/u/yaboulna/mdc/out";
		a = argList.indexOf("-out");
		if (a >= 0) {
			outPath = argList.get(a + 1);
		}

		FileFilter arffFilter = new FileFilter() {
			public boolean accept(File arg0) {
				return arg0.getName().endsWith(".arff");
			}
		};

		String trainingPath = "/u/yaboulna/mdc/train";
		a = argList.indexOf("-train");
		if (a >= 0) {
			trainingPath = argList.get(a + 1);
		}
		File[] trainingFiles = FileUtils.getFile(trainingPath).listFiles(
				arffFilter);

		trainingSet = loadAugmentedInstances(trainingFiles);

		String testPath = "/u/yaboulna/mdc/test";
		a = argList.indexOf("-test");
		if (a >= 0) {
			testPath = argList.get(a + 1);
		}
		testFiles = FileUtils.getFile(testPath).listFiles(arffFilter);

		// testSet = loadAugmentedInstances(testFiles);

		ExecutorService appExec = Executors.newFixedThreadPool(numThreads);

		List<Future<Void>> listFuture = new LinkedList<Future<Void>>();

		for (double s : Arrays.asList(1.0, 1.26, 1.51)) {
			for (int b : Arrays.asList(1, 10, (6 * 24), 1000)) {
				try {
					listFuture.add(appExec.submit(new Test(s, b)));
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		try {
			for (Future<Void> future : listFuture)
				future.get();

			appExec.shutdown();

			while (!appExec.isTerminated()) {
				System.out.println("Shutting down");
				Thread.sleep(5000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private static Instances loadAugmentedInstances(File[] dataFiles)
			throws IOException {
		Instances result = null;
		for (int u = 0; u < dataFiles.length; ++u) {

			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(dataFiles[u]);

			Instances struct = dataLoader.getStructure();
			struct.setClassIndex(struct.numAttributes() - 1);

			if (u == 0) {
				result = new Instances(struct);
			}

			Instance inst;
			while ((inst = dataLoader.getNextInstance(struct)) != null) {
				result.add(inst);
			}
		}
		System.out.println("Loaded " + result.numInstances());
		return result;
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
				} else if (l == 1) {
					trainingSet.instance(i).setClassValue("-1");
				} else {
					if (delete) {
						// trainingSet.delete(i);
						trainingSet.instance(i).setWeight(
								1.0 / trainingSet.numInstances());
					}
				}
				break;
			case 1:
				// case 2:
			case 3:
				if (l == 0) {
					trainingSet.instance(i).setClassValue("-1");
				} else if (l == 1) {
					trainingSet.instance(i).setClassValue("+1");
				} else if (l == 3) {
					if (delete) {
						// trainingSet.delete(i);
						trainingSet.instance(i).setWeight(
								1.0 / trainingSet.numInstances());
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
						// trainingSet.delete(i);
						trainingSet.instance(i).setWeight(
								1.0 / trainingSet.numInstances());
					} else {
						// nothing
					}
				}
				break;
			}

		}
		return trainingSet;
	}

	private static Instances changeStruct(Instances dataStruct)
			throws Exception {
		Instances copyInsts = new Instances(dataStruct); // , 0);
		Add add = new Add();
		add.setAttributeIndex("last");
		add.setAttributeName("binary-label");
		add.setNominalLabels(LABELS_BINARY[0] + "," + LABELS_BINARY[1]);
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

	static void writeEvaluation(int[][] confusionMatrix, Writer evalWr)
			throws IOException {
		double[] precision = new double[11];
		double[] recall = new double[11];
		double avgPrec = 0.0;
		double avgRecall = 0.0;
		for (int i = 0; i < 11; ++i) {
			double numerator = confusionMatrix[i][i];
			double precDenim = 0.0;
			double recDenim = 0.0;
			for (int j = 0; j < 11; ++j) {
				precDenim += confusionMatrix[j][i];
				recDenim += confusionMatrix[i][j];
			}
			precision[i] = numerator / precDenim;
			recall[i] = numerator / recDenim;
			avgPrec += precision[i];
			avgRecall += recall[i];
		}
		evalWr.append(
				"Precision: " + Arrays.toString(precision) + " Average: "
						+ (avgPrec / 11) + "\n").append(
				"Recall: " + Arrays.toString(recall) + " Average: "
						+ (avgRecall / 11) + "\n");

		evalWr.append("Confusion Matrix:\n").append(
				"label\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\n");

		for (int i = 0; i < confusionMatrix.length; ++i) {
			evalWr.append(Integer.toString(i));
			long rowTotal = 0;

			for (int j = 0; j < LABELS.length; ++j) {
				double cnt = confusionMatrix[i][j];
				rowTotal += cnt;
				evalWr.append('\t').append(Long.toString(Math.round(cnt)));
			}

			evalWr.append('\t').append(Long.toString(rowTotal)).append('\n');
		}

	}

	public static void normalizeProbabilities(double[] modifiedLabelDistrib) {
		double sum = 0;
		for (int i = 0; i < modifiedLabelDistrib.length; ++i) {
			if (Double.isNaN(modifiedLabelDistrib[i])) {
				continue;
			}
			sum += modifiedLabelDistrib[i];
		}
		for (int i = 0; i < modifiedLabelDistrib.length; ++i) {
			modifiedLabelDistrib[i] /= sum;
		}
	}
}
