package uwaterloo.mdc.weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import weka.classifiers.Evaluation;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Range;

public class PlaceLabels implements Callable<Void> {

//	String trainingPath =
	
	String inPath = "C:\\mdc-datasets\\weka\\validation\\done_985";
	FileFilter dirFilter = new FileFilter() {

		public boolean accept(File pathname) {
			return pathname.isDirectory();
		}

	};

	public static final String[] LABELS_SINGLES = { "0", "1", "2", "3", "4",
			"5", "6", "7", "8", "9", "10" };
	private static final int MAX_CLUSTERS = 10;
	private static final int MIN_CLUSTERS = 1;
	public final Pattern tabSplit = Pattern.compile("\\t");
	public final Random rand = new Random(System.currentTimeMillis());
	public static final boolean validate = true;
	static Properties groundTruth;

	private final int n;
	private final double superiorityRatio;

	public PlaceLabels(int pN) {
		this.n = pN;
		superiorityRatio = 1.0 + n / 10.0;
	}

	public Void call() throws Exception {

		FastVector attsFV = new FastVector();
		// attsFV.addElement(new Attribute("ID", (FastVector) null));
		for (String label : LABELS_SINGLES) {
			attsFV.addElement(new Attribute(label));
		}
		FastVector labelFV = new FastVector();
		for (String label : LABELS_SINGLES) {
			labelFV.addElement(label);
		}
		attsFV.addElement(new Attribute("label", labelFV));

		File inDir = FileUtils.getFile(inPath);

		for (File classifierDir : inDir.listFiles(dirFilter)) {
			for (File evalDir : classifierDir.listFiles(dirFilter)) {
				Properties finalPredictions = new Properties();
				LinkedList<String> placeIds = new LinkedList<String>();
				TreeMap<String, TreeMap<String, String>> sortPredictions = new TreeMap<String, TreeMap<String, String>>();
				Frequency tpr = new Frequency();

				BufferedReader rd = new BufferedReader(Channels.newReader(
						FileUtils.openInputStream(
								FileUtils.getFile(evalDir,
										"placeid-predictions.csv"))
								.getChannel(), "US-ASCII"));
				try {
					rd.readLine(); // skip header

					Instances predictInsts = new Instances("Predictions",
							attsFV, 0);
					predictInsts
							.setClassIndex(predictInsts.numAttributes() - 1);

					String line;
					while ((line = rd.readLine()) != null) {
						Instance inst = new Instance(
								predictInsts.numAttributes());
						inst.setDataset(predictInsts);

						String[] fields = tabSplit.split(line);
						// inst.setValue(0, fields[0]);
						placeIds.add(fields[0]);
						double maxPct = Double.parseDouble(fields[1]);
						LinkedList<String> probableLabels = new LinkedList<String>();
						for (int f = 1; f < fields.length - 1; ++f) {
							double freq = Double.parseDouble(fields[f]);
							inst.setValue(f - 1, freq);

							double probDiv = freq / maxPct;
							if (probDiv > 1 / superiorityRatio) {
								if (probDiv > superiorityRatio) {
									// This is a pure cluster
									probableLabels.clear();
								}
								probableLabels
										.add(inst.attribute(f - 1).name());
								if (probDiv > 1) {
									maxPct = freq;
								}
							}
							// else if (Math.abs(freq - maxPct) < 0.05) {
							// // This cluster is not pure
							// probableLabels
							// .add(inst.attribute(f - 1).name());
							// }
						}

						String label = probableLabels.get(rand
								.nextInt(probableLabels.size()));
						// if (probableLabels.size() == 1) {
						// label = probableLabels.get(0);
						// } else {
						// label =
						//
						// }
						finalPredictions.put(fields[0], label);

						inst.setWeight(Double
								.parseDouble(fields[fields.length - 1]));
						if (validate) {

							String trueLabel = groundTruth.getProperty(
									fields[0], "0");
							inst.setClassValue(trueLabel);
							if (!"0".equals(trueLabel)) {
								tpr.addValue(label.equals(trueLabel));
							}

						}

						predictInsts.add(inst);

						// Either this code or the oe below
						String placeId = fields[0];
						String userId = placeId.substring(0, 3);
						String placeSeq = placeId.substring(4);
						if (!"0".equals(label)) {
							TreeMap<String, String> userPredictions = sortPredictions
									.get(userId);
							if (userPredictions == null) {
								userPredictions = new TreeMap<String, String>();
								sortPredictions.put(userId, userPredictions);
							}
							userPredictions.put(placeSeq, label);

						}
					}

					// SimpleKMeans km = new SimpleKMeans();
					// km.setSeed(rand.nextInt());
					// km.setNumClusters(n);
					//
					// ClassificationViaClustering finalClassifier = new
					// ClassificationViaClustering();
					// finalClassifier.setClusterer(km);
					// finalClassifier.buildClassifier(predictInsts);
					//
					// // em.buildClusterer(predictInsts);
					// // ClusterEvaluation eval = new ClusterEvaluation();
					// // eval.setClusterer(em);
					// // eval.evaluateClusterer(predictInsts);
					// // int[] classesToClusters = eval.getClassesToClusters();
					// // double[] clusterAssignment =
					// // eval.getClusterAssignments();
					//
					// for (int i = 0; i < predictInsts.numInstances(); ++i) {
					// String placeId = placeIds.get(i);
					// String label = finalPredictions.getProperty(placeId);
					// if (label == null) {
					// label = Long
					// .toString(Math.round(finalClassifier
					// .classifyInstance(predictInsts
					// .instance(i))));
					// // String label = Integer
					// // .toString(classesToClusters[(int) Math
					// // .round(clusterAssignment[i])]);
					// finalPredictions.setProperty(placeId, label);
					// }
					//
					// String userId = placeId.substring(0, 3);
					// String placeSeq = placeId.substring(4);
					// if (!"0".equals(label)) {
					// TreeMap<String, String> userPredictions = sortPredictions
					// .get(userId);
					// if (userPredictions == null) {
					// userPredictions = new TreeMap<String, String>();
					// sortPredictions.put(userId, userPredictions);
					// }
					// userPredictions.put(placeSeq, label);
					// }
					// }

					Writer wr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(evalDir, "n" + n
											+ "_final-predictions.csv"))
									.getChannel(), "US-ASCII"); // "UTF-8");

					try {
						wr.append("userid\tplace_id\tlabel\n");

						for (String userid : sortPredictions.keySet()) {
							TreeMap<String, String> userPredictions = sortPredictions
									.get(userid);
							for (String placeSeq : userPredictions.keySet()) {
								String label = userPredictions.get(placeSeq);
								wr.append(userid).append('\t').append(placeSeq)
										.append('\t').append(label)
										.append('\n');
							}
						}
					} finally {
						wr.flush();
						wr.close();

					}
					OutputStream os = FileUtils.openOutputStream(FileUtils
							.getFile(evalDir, "n" + n
									+ "_final-predictions.properties"));
					try {
						finalPredictions.store(os, null);
						// finalClassifier.toString());
						// "Loglikelihood: " + eval.getLogLikelihood()
						// +"\nCluster assignment: " +
						// Arrays.toString(clusterAssignment)
						// +"\nClasses to Clusters: " +
						// Arrays.toString(classesToClusters));
					} finally {
						os.flush();
						os.close();
					}

					if (validate) {
						// Evaluation eval = new Evaluation(predictInsts);
						// StringBuffer buff = new StringBuffer();
						// eval.crossValidateModel(finalClassifier,
						// predictInsts,
						// 10, rand, buff, new Range("first-last"), true);
						Writer eWr = Channels.newWriter(
								FileUtils.openOutputStream(
										FileUtils.getFile(evalDir, "n" + n
												+ "_evaluation.txt"))
										.getChannel(), "US-ASCII");
						try {
							// eWr.append(eval.toClassDetailsString()).append(
							// "\n\n");
							// eWr.append(eval.toMatrixString()).append("\n\n");
							// eWr.append(eval.toSummaryString()).append("\n\n");
							// eWr.append(buff.toString()).append("\n\n");
							eWr.append("TRUE\tFALSE\tCount\n")
									.append(Double.toString(tpr
											.getPct(Boolean.TRUE)))
									.append('\t')
									.append(Double.toString(tpr
											.getPct(Boolean.FALSE)))
									.append('\t')
									.append(Double.toString(tpr
											.getCount(Boolean.TRUE)
											+ tpr.getCount(Boolean.FALSE)))
									.append('\n');
						} finally {
							eWr.flush();
							eWr.close();
						}
					}
				} finally {
					rd.close();
				}
			}
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		groundTruth = new Properties();
		if (validate) {
			InputStream gtIs = FileUtils.openInputStream(FileUtils
					.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE));
			// inPath, "place-labels.properties"));
			try {
				groundTruth.load(gtIs);
			} finally {
				gtIs.close();
			}
		}
		Future<Void> lastFuture = null;
		ExecutorService appExec = Executors.newFixedThreadPool(MAX_CLUSTERS
				- MIN_CLUSTERS + 1);
		for (int n = MIN_CLUSTERS; n <= MAX_CLUSTERS; ++n) {
			PlaceLabels app = new PlaceLabels(n);
			// app.call();
			lastFuture = appExec.submit(app);
		}
		lastFuture.get();
		appExec.shutdown();
		while (!appExec.isTerminated()) {
			Thread.sleep(1000);
		}
	}
}
