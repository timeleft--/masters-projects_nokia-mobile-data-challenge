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
import org.apache.commons.io.FilenameUtils;

import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.classifiers.Classifier;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.trees.RandomForest;
import weka.core.EuclideanDistance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;

public class Test implements Callable<KeyValuePair<String, double[]>> {
	static Class<? extends Classifier>[] classifiers = new Class[] {
	// DMNBtext.class,
	// DecisionStump.class,
	RandomForest.class,
	// J48.class
	// AdaBoostM1.class,
	// MultiBoostAB.class,
	// LibSVM.class
	};
	private static final String[] LABELS = new String[] { "0", "1", "2", "3",
			"4", "5", "6", "7", "8", "9", "10" };
	public static final String[] LABELS_BINARY = new String[] { "+1", "-1" };
	public static final int LABLES_BINARY_POSITIVE_IX = 0;
	public static final int LABLES_BINARY_NEGATIVE_IX = 1;
	private static final boolean validate = false;
	private static final Properties placeLabels = new Properties();
	private static final boolean verbose = false;
	private static final int numFolds = 4;
	private static final boolean DIM_REDUCE = false;
	private static final boolean MAXIMIZE_ENTROPY = false;
	private static boolean crossValidate = false;

	private static int numThreads;
	private static Instances trainingSet;
	private static File[] testFiles;
	private static String outPath;

	double supRatio;
	int backLength;
	Random rand = new Random(System.currentTimeMillis());
	Class<? extends Classifier> classifierClazz;

	public Test(double s, int b, Class<? extends Classifier> c) {
		supRatio = s;
		backLength = b;
		this.classifierClazz = c;
	}

	public KeyValuePair<String, double[]> call() throws Exception {

		Map<File, double[][]> userInstDistribs = new HashMap<File, double[][]>();
		Map<File, int[]> userHonoredClassifier = new HashMap<File, int[]>();
		Map<String, double[]> placeDistribsMap = new TreeMap<String, double[]>();
		Map<String, Integer> placeCountMap = new TreeMap<String, Integer>();
		long totalCount = 0;

		for (int l = 0; l < 4; ++l) {
			Instances training = copyInstancesIntoSets(trainingSet, l, true);
			Classifier cls = classifierClazz.getConstructor().newInstance();

			if (DIM_REDUCE) {
				AttributeSelectedClassifier atsc = new AttributeSelectedClassifier();
				atsc.setClassifier(cls);
				atsc.setEvaluator(new PrincipalComponents());
				atsc.setSearch(new Ranker());
				cls = atsc;
			}

			cls.buildClassifier(training);

			for (File userFile : testFiles) {

				Instances test = new Instances(Channels.newReader(FileUtils
						.openInputStream(userFile).getChannel(), "US-ASCII"));
				test.setClassIndex(test.numAttributes() - 1);
				test = copyInstancesIntoSets(test, l, false);

				String filenamePfx = test.relationName() + "_n" + supRatio
						+ "_b" + backLength + "_l" + l + "_ALL";

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

								int p = (c == 1 || c == 3 || c == 2? LABLES_BINARY_POSITIVE_IX
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
							if (honoredClassifier[i] > 3) {
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
							if (verbose) {
								System.out.println("Replacing "
										+ Arrays.toString(dist) + " by "
										+ Arrays.toString(instDistribs[i]));
							}
							dist = instDistribs[i];

							double[] placeDistribs = placeDistribsMap
									.get(placeId);
							if (placeDistribs == null) {
								placeDistribs = new double[LABELS.length];
								placeDistribsMap.put(placeId, placeDistribs);
							}

							for (int p = 0; p < placeDistribs.length; ++p) {
								if (dist[p] > 0) {
									placeDistribs[p] += dist[p];
								}
							}

							Integer placeCount = placeCountMap.get(placeId);
							if (placeCount == null) {
								placeCount = 0;
							}
							placeCountMap.put(placeId, placeCount + 1);
							++totalCount;
						}

						double label = getClassFromDistrib(dist, tInst,
								withinDayInsts, distanceMeasure);
						String line = instIDVisitTimeRd.readLine();
						if (!line.startsWith(test.relationName() + "\t"
								+ instId)) {
							throw new AssertionError(
									"Visit times and labels are not inline");
						}
						instIDVisitTimeWr.append(line + "\t" + label + "\t\""
								+ Arrays.toString(dist) + "\"\n");
						prevInstLabel = label;
					}

				} finally {
					instIDVisitTimeWr.flush();
					instIDVisitTimeWr.close();

					instIDVisitTimeRd.close();
				}
			}
		}

		double[] beforeEM = writePlaceSummary(placeDistribsMap, placeCountMap,
				"beforeEM");

		em(placeDistribsMap, placeCountMap, totalCount);

		double[] afterEM = writePlaceSummary(placeDistribsMap, placeCountMap,
				"afterEM");

		double[] result = new double[6];
		for (int i = 0; i < 6; ++i) {
			if (i % 2 == 0) {
				result[i] = beforeEM[i / 2];
			} else {
				result[i] = afterEM[i / 2];
			}
		}
		return new KeyValuePair<String, double[]>(supRatio + "\t" + backLength,
				result);
	}

//	double[] labelPriors = new double[] 	
//			{ 1E-9, 0.15, 0.1-1E-9, 0.15, 0.05, 0.1,
//			0.1, 0.1, 0.1, 0.1, 0.05 };
//	{ 0.2, 0.15, 0.1, 0.15, 0.05, 0.05,
//		0.05, 0.05, 0.05, 0.05, 0.05 };
//			{ 0.1, 0.1, 0.1, 0.1, 0.05, 0.1,
//			0.1, 0.1, 0.1, 0.1, 0.05 };
//	{ 0.5, 0.15, 0.1, 0.15, 0.01, 0.02,
//		0.02, 0.02, 0.01, 0.01, 0.01 };
	// { 0.9, 0.02, 0.01, 0.02, 0.0075,
		// 0.0075, 0.0075, 0.0075, 0.0075, 0.0075, 0.005 };


	double[] prevalentPriors = new double[]  	
	{ 1E-9, 0.5, 0.1, 0.33-1E-9, 0.01, 0.01,
			0.01, 0.01, 0.01, 0.01, 0.01 };
	double[] rarePriors = new double[] 
			{ 1E-9, 1E-4, 0.15-(1E-9+2*1E-4), 1E-4, 0.05, 0.15,
			0.15, 0.15, 0.15, 0.15, 0.05 };

	private void em(Map<String, double[]> placeDistribsMap,
			Map<String, Integer> placeCountMap, double totalCount) {

		double avgCount = totalCount *  1.0 / placeCountMap.size();
		double logLk = 0.0;
		double sumOfW = 0.0;
		double logLkOld = 0;
		for (int i = 0; i < 1000; ++i) {

			Map<String, double[]> emWeightsMap = new HashMap<String, double[]>();
			logLkOld = logLk;

			logLk = 0.0;
			sumOfW = 0.0;
			for (String placeId : placeDistribsMap.keySet()) {
				double[] emWeights = emWeightsMap.get(placeId);
				if (emWeights == null) {
					emWeights = new double[11];
					emWeightsMap.put(placeId, emWeights);
				}

				int count = placeCountMap.get(placeId);
			
				double[] priors;
				if(count > avgCount){
					priors = prevalentPriors;
				} else {
					priors = rarePriors;
				}
//				priors = labelPriors;
				
				double[] placeDistrib = placeDistribsMap.get(placeId);
				
				// Start messing up
				placeDistrib[0] = 0;
				normalizeProbabilities(placeDistrib);
				// End messing up
				
				double[] logJoint = new double[LABELS.length];
				double max = Double.MIN_VALUE;
				for (int c = 0; c < LABELS.length; ++c) {
					if (count > 0 && placeDistrib[c] > 0) {
						if (MAXIMIZE_ENTROPY) {
							logJoint[c] = priors[c]
									* ((placeDistrib[c] / count) * (Math
											.log(count) - Math
											.log(placeDistrib[c])));
						} else {
							logJoint[c] = Math.log(placeDistrib[c] / count)
									+ Math.log(priors[c]);
						}
					}
					if (logJoint[c] > max) {
						max = logJoint[c];
						// maxIx = c;
					}
				}
				double sum = 0.0;
				for (int c = 0; c < LABELS.length; ++c) {
					emWeights[c] = Math.exp(logJoint[c] - max);
					sum += emWeights[c];
				}

				double logDensity = (max + Math.log(sum));
				for (int c = 0; c < LABELS.length; ++c) {
					
					double w = 1;//(avgCount / count);
					
					logLk += w * logDensity;
					sumOfW += w;

					emWeights[c] /= sum;
				}
			}

			logLk /= sumOfW;

			if (i > 0) {
				if ((logLk - logLkOld) < 1E-6) {
					break;
				}
			}

			for (String placeId : placeDistribsMap.keySet()) {
				double[] emWeights = emWeightsMap.get(placeId);
				double[] placeDistrib = placeDistribsMap.get(placeId);
				for (int c = 0; c < LABELS.length; ++c) {
					placeDistrib[c] *= emWeights[c];
				}
			}
		}
	}

	private double[] writePlaceSummary(Map<String, double[]> placeDistribsMap,
			Map<String, Integer> placeCountMap, String measure)
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

				double[] placeDist = placeDistribsMap.get(placeId);
				int placeCount = placeCountMap.get(placeId);
				wr.append(placeCount + "\t");

				double maxProb = Double.MIN_VALUE;
				LinkedList<Integer> result = new LinkedList<Integer>();

				for (int c = 0; c < LABELS.length; ++c) {

					double prob = placeDist[c] / placeCount;

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

		double[] result;
		wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath, "n" + supRatio + "_b"
								+ backLength + "_evaluation_" + measure
								+ ".csv")).getChannel(), "US-ASCII");
		try {
			result = writeEvaluation(confusion, wr);
		} finally {
			wr.flush();
			wr.close();
		}
		return result;
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

		String testPath = "/u/yaboulna/mdc/test";
		a = argList.indexOf("-test");
		if (a >= 0) {
			testPath = argList.get(a + 1);
		}

		if (crossValidate) {
			String outPathOrig = outPath;
			int foldWidth = trainingFiles.length / numFolds;
			for (int v = 0; v < numFolds; ++v) {
				LinkedList<File> foldTraining = new LinkedList<File>(
						Arrays.asList(trainingFiles));

				int foldStart = v * foldWidth;
				int foldEnd = foldStart + foldWidth;
				LinkedList<File> foldTesting = new LinkedList<File>();
				for (int f = foldEnd - 1; f >= foldStart; --f) {
					foldTesting.add(foldTraining.remove(f));
				}
				testFiles = new File[foldWidth];
				foldTesting.toArray(testFiles);
				File[] temp = new File[foldTraining.size()];
				foldTraining.toArray(temp);
				trainingSet = loadAugmentedInstances(temp);
				outPath = FilenameUtils.concat(outPathOrig, "v" + v);
				runFold();
			}
		} else {
			trainingSet = loadAugmentedInstances(trainingFiles);
			testFiles = FileUtils.getFile(testPath).listFiles(arffFilter);
			runFold();
		}

	}

	private static void runFold() throws IOException {
		String outPathOrig = outPath;

		for (Class c : classifiers) {
			outPath = FilenameUtils.concat(outPathOrig, c.getName());
			outPath = FilenameUtils.concat(outPath, "info-gain");

			ExecutorService appExec = Executors.newFixedThreadPool(numThreads);

			List<Future<KeyValuePair<String, double[]>>> listFuture = new LinkedList<Future<KeyValuePair<String, double[]>>>();

			for (double s : Arrays.asList(1.0,1.15)){
					//1.0, 1.05, 1.10, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4, 1.45/* , 1.51, 2.0 */)) {
				for (int b : Arrays.asList(0,1,2)){
//						1, 2,3,4,5,6,7,8,9,10/* , (6 * 24), 1000 */)) {
					try {
						listFuture.add(appExec.submit(new Test(s, b, c)));
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}

			Writer wr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, "n_b_eval.csv"))
							.getChannel(), "US-ASCII");
			try {
				wr.append("n\tb\ttcr\ttcr_em\tprecision\tprecision_em\trecall\trecall_em\n");

				for (Future<KeyValuePair<String, double[]>> future : listFuture) {
					KeyValuePair<String, double[]> res = future.get();
					wr.append(res.getKey());
					double[] measures = res.getValue();
					for (int i = 0; i < measures.length; ++i) {
						wr.append("\t" + measures[i]);
					}
					wr.append("\n");
				}

				appExec.shutdown();

				while (!appExec.isTerminated()) {
					System.out.println("Shutting down");
					Thread.sleep(5000);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				wr.flush();
				wr.close();
			}
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
			case 2:
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

	static double[] writeEvaluation(int[][] confusionMatrix, Writer evalWr)
			throws IOException {
		double[] precision = new double[11];
		double[] recall = new double[11];
		double tc = 0;
		double grandTotal = 0;
		double avgPrec = 0.0;
		double avgRecall = 0.0;
		for (int i = 0; i < 11; ++i) {
			double numerator = confusionMatrix[i][i];
			tc += numerator;
			double precDenim = 0.0;
			double recDenim = 0.0;
			for (int j = 0; j < 11; ++j) {
				precDenim += confusionMatrix[j][i];
				recDenim += confusionMatrix[i][j];
			}
			if (precDenim > 0) {
				precision[i] = numerator / precDenim;
			}
			if (recDenim > 0) {
				recall[i] = numerator / recDenim;
			}
			avgPrec += precision[i];
			avgRecall += recall[i];
		}
		avgPrec /= 11.0;
		avgRecall /= 11.0;
		evalWr.append(
				"Precision: " + Arrays.toString(precision) + " Average: "
						+ avgPrec + "\n").append(
				"Recall: " + Arrays.toString(recall) + " Average: " + avgRecall
						+ "\n");

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
			grandTotal += rowTotal;
			evalWr.append('\t').append(Long.toString(rowTotal)).append('\n');
		}

		tc /= grandTotal;
		evalWr.append("Truce Classification Rate: " + tc);

		return new double[] { tc, avgPrec, avgRecall };
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
