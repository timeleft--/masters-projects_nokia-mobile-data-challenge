package uwaterloo.mdc.weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Config.NORMALIZE_BY_ENUM;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.mallet.LoadInputsIntoDocs;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;
import uwaterloo.util.NotifyStream;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.converters.ArffSaver;
import weka.core.converters.SVMLightSaver;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;

//import uwaterloo.mdc.etl.mallet.*;

public class LoadCountsAsAttributes implements
		Callable<ExecutorCompletionService<Instances>> {

	public static final String CLASSIFIER_TO_HONOUR = "weka.classifiers.trees.J48";
	public static final String ATRRSELECTOR_TO_HONOUR = "weka.attributeSelection.SVMAttributeEval";

	private static class SVMLightSaverSingleton implements Callable<Void> {
		protected static List<String> userIds;
		protected static final String SVMLIGHT_PARTIALOUT_PATH = "C:\\mdc-datasets\\svmlight-temp\\";
		protected static SVMLightSaverSingleton singleton;
		protected static Thread lockedFor;

		public static synchronized SVMLightSaverSingleton acquireSaver(
				Instances insts) {
			if (lockedFor == null) {
				lockedFor = Thread.currentThread();
				singleton.insts = insts;
				return singleton;
			} else {
				return null;
			}
		}

		protected String outPath;
		protected Writer[] outWriter;
		protected Instances insts;
		private int numFolds;

		protected SVMLightSaverSingleton(String outPath, int numFolds)
				throws IOException {
			this.outPath = outPath;
			this.numFolds = numFolds;
			outWriter = new Writer[numFolds];
			for (int v = 0; v < numFolds; ++v) {
				outWriter[v] = Channels.newWriter(
						FileUtils
								.openOutputStream(
										FileUtils.getFile(outPath, "v" + v,
												"input.csv")).getChannel(),
						Config.OUT_CHARSET);
			}
		}

		public static void init(String inputPath, int numFolds)
				throws IOException {
			init(inputPath, SVMLIGHT_PARTIALOUT_PATH, numFolds);
		}

		private static void init(String inputPath, String outPath, int numFolds)
				throws IOException {
			if (singleton != null) {
				throw new UnsupportedOperationException("Cannot re-init");
			}
			userIds = Arrays.asList(FileUtils.getFile(inputPath).list());
			Collections.sort(userIds);
			singleton = new SVMLightSaverSingleton(outPath, numFolds);
		}

		public void dispose() throws IOException {
			singleton = null;
			for (int i = 0; i < outWriter.length; ++i) {
				outWriter[i].flush();
				outWriter[i].close();
			}
		}

		public void release() {
			lockedFor = null;
		}

		@Override
		public Void call() throws Exception {
			// TODO support more than LOO
			int validationUserIx = userIds.indexOf(insts.relationName());
			for (int v = 0; v < numFolds; ++v) {
				Writer wr;
				if (v == validationUserIx) {
					wr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, "v" + v,
											"validate.csv")).getChannel(),
							Config.OUT_CHARSET);
				} else {
					wr = outWriter[v];
				}

				@SuppressWarnings("rawtypes")
				Enumeration instEnum = insts.enumerateInstances();
				while (instEnum.hasMoreElements()) {
					Instance currInst = (Instance) instEnum.nextElement();
					wr.append(getLinePrefix(currInst, v, validationUserIx));

					// Enumeration attrsEnum = currInst.enumerateAttributes();
					// while(attrsEnum.hasMoreElements()){
					// Attribute attr = (Attribute) attrsEnum.nextElement();
					// if(currInst.isMissing(attr) || currInst.classAttribute()
					// ==
					// attr)
					double[] attrs = currInst.toDoubleArray();
					int classIx = currInst.classIndex();
					// missing is NaN and cannot be used in equalities double
					// missing = Instance.missingValue();
					for (int a = 0; a < attrs.length; ++a) {
						if (a == classIx || Double.isNaN(attrs[a])) {
							continue;
						}
						wr.append(" ")
								.append(Integer.toString(a
										+ getFeatureNumShift())).append(":")
								.append(Double.toString(attrs[a]));
					}
					wr.append(System.lineSeparator()); // TODO: will
														// svmlight
														// understand
														// this
														// or '\n'?
				}
				if (v == validationUserIx) {
					wr.flush();
					wr.close();
				}
			}
			return null;
		}

		protected int getFeatureNumShift() {
			return 1;
		}

		protected String getLinePrefix(Instance currInst, int v,
				int validationUserIx) throws IOException {
			String result;
			if (currInst.classIsMissing()) {
				result = "0";
			} else {
				result = Long.toString(Math.round(currInst.classValue()));
			}
			if (result == null || "null".equals(result)) {
				throw new NullPointerException("where did this come from??");
			}
			return result;
		}

	}

	private static class AppsSVMLightSaver extends SVMLightSaverSingleton {
		private int featureNumShift;
		private BufferedReader[] partialReader;
		private BufferedReader[] validationReader;

		protected AppsSVMLightSaver(String outPath, int numFolds,
				int featureNumShift) throws IOException {
			super(outPath, numFolds);
			this.featureNumShift = featureNumShift;
			partialReader = new BufferedReader[numFolds];
			validationReader = new BufferedReader[numFolds];
			for (int v = 0; v < numFolds; ++v) {
				partialReader[v] = new BufferedReader(
						Channels.newReader(
								FileUtils
										.openInputStream(
												FileUtils
														.getFile(
																SVMLightSaverSingleton.SVMLIGHT_PARTIALOUT_PATH,
																"v" + v,
																"input.csv"))
										.getChannel(), Config.OUT_CHARSET));

				validationReader[v] = new BufferedReader(
						Channels.newReader(
								FileUtils
										.openInputStream(
												FileUtils
														.getFile(
																SVMLightSaverSingleton.SVMLIGHT_PARTIALOUT_PATH,
																"v" + v,
																"validate.csv"))
										.getChannel(), Config.OUT_CHARSET));
			}

		}

		public static void init(String inputPath, String outPath, int numFolds,
				int featureNumShift) throws IOException {
			if (singleton != null) {
				throw new UnsupportedOperationException("Cannot re-init");
			}
			userIds = Arrays.asList(FileUtils.getFile(inputPath).list());
			Collections.sort(userIds);
			singleton = new AppsSVMLightSaver(outPath, numFolds,
					featureNumShift);
		}

		@Override
		protected int getFeatureNumShift() {
			return featureNumShift;
		}

		@Override
		protected String getLinePrefix(Instance currInst, int v,
				int validationUserIx) throws IOException {

			BufferedReader rd;

			if (v == validationUserIx) {
				rd = validationReader[v];
			} else {
				rd = partialReader[v];
			}

			String result = rd.readLine();
			// if (result == null) {
			// throw new IndexOutOfBoundsException(
			// "The files of the apps and the other input should be the same length");
			// }
			return result;
		}
	}

	public static final String OUTPUT_PATH = "C:\\mdc-datasets\\weka\\segmented_user_sample-noweight";
	public static final String TEMP_PATH = "C:\\mdc-datasets\\weka\\segmented_user_temp";

	private static class ArffSaverCallable implements Callable<Void> {
		final String outPath;
		Instances insts;
		private boolean arffFmt = true;
		boolean temp = true;
		int placeIdAttrIx;

		public ArffSaverCallable(Instances insts, String outPath,
				boolean arffFormat, boolean pTemp, int pPlaceIdAttrIx) {
			this(insts, outPath);
			arffFmt = arffFormat;
			temp = pTemp;
			this.placeIdAttrIx = pPlaceIdAttrIx;
		}

		public ArffSaverCallable(Instances insts, String outPath) {
			this.insts = insts;
			this.outPath = FilenameUtils.concat(outPath,
			// FilenameUtils.concat(BASE_ARFFS_PATH, outPath),
					insts.relationName());
		}

		void append(Instances sample, Instances fold) {
			Enumeration instEnum = fold.enumerateInstances();
			while (instEnum.hasMoreElements()) {
				Instance inst = (Instance) instEnum.nextElement();
				sample.add(inst);
				inst.setDataset(sample);
			}
		}

		@Override
		public Void call() throws Exception {
			File dest = FileUtils.getFile(outPath
					+ (arffFmt ? ".arff" : ".csv"));
			System.out.println((System.currentTimeMillis() - time)
					+ ": Writing " + dest.getAbsolutePath());

			if (!temp) {
				if (Config.LOAD_COUNT_WEIGHT) {
					Enumeration instEnum = insts.enumerateInstances();
					while (instEnum.hasMoreElements()) {
						Instance inst = (Instance) instEnum.nextElement();

						if (Config.LOAD_WEIGHT_LABELS) {
							double weight = (1.0 * Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT)
									/ (userClassCount.get(insts.relationName())
											.getCount(Long.toString(Math
													.round(inst.classValue()))));
							inst.setWeight(weight);
						}
						if (Config.LOAD_WEIGHT_USERS) {
							double weight = inst.weight()
									* (1.0 * Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT / insts
											.numInstances());
							inst.setWeight(weight);
						}
					}
				}
				if (Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER) {

					File validationDest = FileUtils.getFile(
							dest.getParentFile(), "validation", dest.getName());
					doSave(validationDest);

					if (insts.numInstances() > Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT) {

						int numSamplesFromPrevalent = Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT;
						for (int c = 4; c <= 10; ++c) {
							numSamplesFromPrevalent -= userClassCount.get(
									insts.relationName()).getCount(
									Integer.toString(c));
						}

						Instances sample = new Instances(
								insts,
								Config.LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT);
						insts = new Instances(insts);

						for (int i = 0; i < insts.numInstances(); ++i) {
							Instance inst = insts.instance(i);
							if (inst.classValue() > 3) {
								sample.add(inst);
								inst.setDataset(sample);
								insts.delete(i);
								--i; // because we deleted one
							}
						}

						Random rand = new Random(System.currentTimeMillis());

						// int i = rand.nextInt(insts.numInstances());
						// Instance inst = insts.instance(i);
						// .. add
						// insts.delete(i);

						int numFolds = (int) Math.round(Math.floor(1.0 * insts
								.numInstances() / 100));
						int numSampleFolds = (int) Math.round(Math
								.floor(1.0 * numSamplesFromPrevalent / 100));
						if (numFolds >= 1.5 * numSampleFolds) {
							insts.stratify(numFolds);
							Set<Integer> appended = new HashSet<Integer>();
							for (int s = 0; s < numSampleFolds; ++s) {
								// && insts.numInstances() > 0;

								Integer r;
								do {
									r = rand.nextInt(numFolds);
								} while (appended.contains(r));
								appended.add(r);
								Instances fold = insts.testCV(numFolds, r);
								append(sample, fold);
							}
						} else {
							append(sample, insts);
						}

						insts = sample;
					}
				} else if (Config.LOAD_USE_PLACEID) {
					Instances copy = new Instances(insts, 0);
					copy.deleteAttributeAt(placeIdAttrIx);
					copy.setClassIndex(copy.numAttributes() - 1);

					TreeMap<String, SummaryStatistics[]> agregateCounts = new TreeMap<String, SummaryStatistics[]>();
					Enumeration instEnum = insts.enumerateInstances();
					while (instEnum.hasMoreElements()) {
						Instance inst = (Instance) instEnum.nextElement();

						String placeId = inst.stringValue(placeIdAttrIx);
						SummaryStatistics[] countSummary = agregateCounts
								.get(placeId);
						// Silently dies inst.deleteAttributeAt(placeIdAttrIx);
						if (countSummary == null) {
							countSummary = new SummaryStatistics[inst
									.numAttributes() - 2]; // the label adnt he
															// id
							for (int s = 0; s < countSummary.length; ++s) {
								countSummary[s] = new SummaryStatistics();
							}
							agregateCounts.put(placeId, countSummary);
						}

						for (int a = 0; a < copy.numAttributes() - 1; ++a) {
							// FIXME
							if (!Config.LOAD_REPLACE_MISSING_VALUES) {
								throw new AssertionError();
							}
							countSummary[a].addValue(inst
									.value((a < placeIdAttrIx ? a : a + 1)));
						}
					}

					Properties instIdPlaceId = new Properties();
					double instId = 1.0;

					for (String placeId : agregateCounts.keySet()) {
						SummaryStatistics[] countSummary = agregateCounts
								.get(placeId);
						Instance inst = new Instance(countSummary.length + 1); // label
						inst.setDataset(copy);
						for (int a = 0; a < countSummary.length; ++a) {
							inst.setValue(a, countSummary[a].getMean());
						}
						// FIXME
						if (!Config.LOAD_MISSING_CLASS_AS_OTHER) {
							throw new AssertionError();
						}
						inst.setClassValue(Config.placeLabels.getProperty(
								placeId, "0"));

						instIdPlaceId.setProperty(Double.toString(instId),
								placeId);
						++instId;

						copy.add(inst);
					}
					insts = copy;
					storeInstIdPlaceId(instIdPlaceId, insts.relationName());
				}
			}

			doSave(dest);
			// System.out.println((System.currentTimeMillis() - time)
			// + ": Finished writing " + dest.getAbsolutePath());

			return null;
		}

		void doSave(File dest) throws IOException {
			if (arffFmt) {
				ArffSaver arffsaver = new ArffSaver();
				arffsaver.setInstances(insts);
				arffsaver.setDestination(FileUtils.openOutputStream(dest));
				arffsaver.setCompressOutput(true);
				arffsaver.writeBatch();
			} else {
				SVMLightSaver svmlightSave = new SVMLightSaver();
				svmlightSave.setInstances(insts);
				svmlightSave.setDestination(FileUtils.openOutputStream(dest));
				svmlightSave.writeBatch();
			}
		}
	}

	private static final String NOMINAL = "NOMINAL";

	private static long time = System.currentTimeMillis();

	private String shotColNamesPath = "C:\\mdc-datasets\\short-col-names.properties";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";

	private Properties shortColNameDict = new Properties();
	private ExecutorService countExec;
	private ExecutorCompletionService<Instances> countEcs;
	private static ExecutorService printExec;
	private static ExecutorCompletionService<Void> printEcs;

	private static Properties featSelectedApps;
	private static FastVector appUsageAttrsFV;
	private static final Map<String, Attribute> appUsageAttrs = Collections
			.synchronizedMap(new HashMap<String, Attribute>());

	private Map<String, HashMap<String, Attribute>> valueDomainMap;
	private FastVector allAttributes;
	private HashSet<String> nominalAttrs;
	private Attribute labelAttribute;
	private static Attribute placeIdAttr;
	private Attribute prevLabelAttribute;
	private Attribute[] prevLabelAttributeArr;
	private int countingJobs;

	private static final Map<String, HashMap<String, HashMap<String, Integer>>> allUsersAppFreqMap = Collections
			.synchronizedMap(new HashMap<String, HashMap<String, HashMap<String, Integer>>>());

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig,
				"LoadCountsAsAttributes");
		try {
			System.setErr(new PrintStream(notifyStream));

			Config.placeLabels = new Properties();
			Config.placeLabels.load(FileUtils.openInputStream(FileUtils
					.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));
			Config.quantizedFields = new Properties();
			Config.quantizedFields.load(FileUtils.openInputStream(FileUtils
					.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));

			featSelectedApps = new Properties();
			featSelectedApps
					.load(FileUtils.openInputStream(FileUtils
							.getFile((Config.LOAD_FEAT_SELECTED_APPS_ONLY ? Config.FEAT_SELECTED_APPS_PATH
									: Config.APPUID_PROPERTIES_FILE))));

			LoadCountsAsAttributes app = new LoadCountsAsAttributes();

			if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {
				printExec = Executors.newSingleThreadExecutor();
				SVMLightSaverSingleton.init(app.inputPath,
						Config.NUM_USERS_TO_PROCESS); // TODO: support more than
														// LOO
			} else {
				printExec = Executors.newFixedThreadPool(Config.NUM_THREADS);
				printEcs = new ExecutorCompletionService<Void>(printExec);
			}

			ExecutorCompletionService<Instances> ecs = app.call();
			// int printingJobs = 0;
			for (int c = 0; c < app.countingJobs; ++c) {
				Future<Instances> instsFuture = ecs.take();

				if (instsFuture == null) {
					System.err.println("null future!!!");
					continue;
				}
				Instances insts = instsFuture.get();
				if (insts == null) {
					System.err.println("null insts!!!");
					continue;
				}

				if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {
					SVMLightSaverSingleton svmlightSaver = null;
					while ((svmlightSaver = SVMLightSaverSingleton
							.acquireSaver(insts)) == null) {
						Thread.sleep(500);
					}
					try {
						printExec.submit(svmlightSaver).get();
					} finally {
						svmlightSaver.release();
					}
				} else {
					printExec.submit(new ArffSaverCallable(insts, TEMP_PATH))
							.get();
					// ++printingJobs;

				}
			}

			if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {

				SVMLightSaverSingleton noncontended = SVMLightSaverSingleton
						.acquireSaver(null);
				noncontended.dispose();
				noncontended.release();
			} else {
				// if (printingJobs != app.countingJobs) {
				// System.err.println("You printed only " + printingJobs
				// + " ARFFs, you needed: " + app.countingJobs);
				// }
			}

			File dataDir = FileUtils.getFile(app.inputPath);
			if (Config.LOADCOUNT_APP_USAGE) {
				// Handle App counts
				int maxAppUsageOccurs = Integer.MIN_VALUE;
				HashMap<String, Integer> aggregateAppFreq = new HashMap<String, Integer>();

				int userIx = 0;
				for (File userDir : dataDir.listFiles()) {
					if (userIx == Config.NUM_USERS_TO_PROCESS) {
						break;
					}
					HashMap<String, HashMap<String, Integer>> appFreqMap = allUsersAppFreqMap
							.get(userDir.getName());
					for (File visit : userDir.listFiles()) {
						for (File microLFile : visit.listFiles()) {
							HashMap<String, Integer> microLocAppFreq = appFreqMap
									.get(microLFile.getName() + visit.getName());
							for (String appUid : microLocAppFreq.keySet()) {
								Integer appCnt = aggregateAppFreq.get(appUid);
								if (appCnt == null) {
									appCnt = 0;
								}
								int newFreq = appCnt
										+ microLocAppFreq.get(appUid);
								aggregateAppFreq.put(appUid, newFreq);
								if (newFreq > maxAppUsageOccurs) {
									maxAppUsageOccurs = newFreq;
								}
							}
						}
					}
					++userIx;
				}

				System.out.println("\nAggregate App Counts:\n"
						+ aggregateAppFreq.toString());

				Percentile appUsagePct = new Percentile();
				double[] aggregateAppFreqVals = new double[aggregateAppFreq
						.size()];
				int d = 0;
				for (Integer appUsage : aggregateAppFreq.values()) {
					aggregateAppFreqVals[d++] = appUsage;
				}

				appUsagePct.setData(aggregateAppFreqVals);
				long stopWordAppTh = Math.round(appUsagePct
						.evaluate(Config.APP_USAGE_FREQ_PERCENTILE_MAX));
				System.out.println("Stop Word Apps Threshold (90 percentile): "
						+ stopWordAppTh);
				long rareWordAppTh = Math.round(appUsagePct
						.evaluate(Config.APP_USAGE_FREQ_PERCENTILE_MIN));
				System.out.println("Rare Apps Threshold (25 percentile): "
						+ rareWordAppTh);

				appUsageAttrsFV = new FastVector();
				for (String appUid : aggregateAppFreq.keySet()) {
					Integer appUsageOccurs = aggregateAppFreq.get(appUid);
					if ((Config.LOAD_DROP_VERYFREQUENT_VALS && appUsageOccurs > stopWordAppTh)
							|| (Config.LOAD_DROP_VERYRARE_VALS && appUsageOccurs < rareWordAppTh)) {
						continue;
					}

					Attribute appAttr = new Attribute(appUid);
					appUsageAttrs.put(appUid, appAttr);
					appUsageAttrsFV.addElement(appAttr);
				}

				if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {
					// TODO support more than LOO
					AppsSVMLightSaver.init(app.inputPath,
							Config.SVMLIGHT_INPUTPATH,
							Config.NUM_USERS_TO_PROCESS,
							app.allAttributes.size());
				}
			}
			LinkedList<Future<Integer>> transformFutures = new LinkedList<Future<Integer>>();
			ExecutorService tranformExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			int userIx = 0;
			for (File userDir : dataDir.listFiles()) {
				if (userIx == Config.NUM_USERS_TO_PROCESS) {
					break;
				}

				transformFutures.add(tranformExec
						.submit(new InstancesTransformCallable(userDir,
								placeIdAttr)));

				++userIx;
			}
			int pendingIOJobs = 0;
			for (Future<Integer> addedPrintTasks : transformFutures) {
				pendingIOJobs += addedPrintTasks.get();
			}

			tranformExec.shutdown();

			if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {

				AppsSVMLightSaver noncontended = (AppsSVMLightSaver) AppsSVMLightSaver
						.acquireSaver(null);
				noncontended.dispose();
				noncontended.release();
			}
			if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE
					|| Config.LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER) {
				File outputDir = FileUtils.getFile(Config.SVMLIGHT_INPUTPATH);
				for (File srcDir : outputDir.listFiles()) {
					for (File srcFile : FileUtils.listFiles(srcDir,
							new String[] { "csv" }, false)) {
						// classes
						String positiveClass = srcFile.getName().substring(1);
						int positiveExamples = 0;
						File inputDir = FileUtils.getFile(
								srcDir.getParentFile(), "c" + positiveClass,
								srcDir.getName());
						Writer destWr = Channels.newWriter(
								FileUtils.openOutputStream(
										FileUtils.getFile(inputDir,
												srcFile.getName()))
										.getChannel(), Config.OUT_CHARSET);
						try {
							BufferedReader srcRead = new BufferedReader(
									Channels.newReader(FileUtils
											.openInputStream(srcFile)
											.getChannel(), Config.OUT_CHARSET));
							String line;
							while ((line = srcRead.readLine()) != null) {
								int spaceIx = line.indexOf(' ');
								String cls = line.substring(0, spaceIx);

								if (positiveClass.contains("+" + cls + "+")) {
									destWr.append("+1")
											.append(line.substring(spaceIx))
											.append(System.lineSeparator());
									if (positiveExamples == 0) {
										System.out
												.println("The first positive example for "
														+ positiveClass
														+ " in "
														+ srcFile
																.getAbsolutePath());
									}
									++positiveExamples;

								} else if (line.startsWith("0")) {
									if (Config.LOADCOUNTS_FOR_SVMLIGHT_TRANSDUCTIVE) {
										destWr.append(line).append(
												System.lineSeparator());
									}
								} else if (positiveClass.contains("-" + cls
										+ "-")) {
									destWr.append("-1")
											.append(line.substring(spaceIx))
											.append(System.lineSeparator());
								}
							}
						} finally {
							destWr.flush();
							destWr.close();
						}

						if (positiveExamples > 0) {

							// Also prepare files for SVMlight output.. it needs
							// a
							// file
							// to overwrite!

							String outDirPathPfx = "c" + positiveClass
									+ File.separator + srcDir.getName()
									+ File.separator + "t";

							for (int t = 0; t < 4; ++t) {
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																Config.SVMLIGHT_OUTPUTPATH,
																outDirPathPfx
																		+ t,
																"alpha.txt"))
										.close();
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																Config.SVMLIGHT_OUTPUTPATH,
																outDirPathPfx
																		+ t,
																"model.txt"))
										.close();
								FileUtils
										.openOutputStream(
												FileUtils
														.getFile(
																Config.SVMLIGHT_OUTPUTPATH,
																outDirPathPfx
																		+ t,
																"trans.txt"))
										.close();
								FileUtils.openOutputStream(
										FileUtils.getFile(
												Config.SVMLIGHT_OUTPUTPATH,
												outDirPathPfx + t,
												"predictions.txt")).close();
							}
						} else {
							System.out.println("No positive examples for "
									+ positiveClass + " in "
									+ srcFile.getAbsolutePath() + ". Deleting "
									+ inputDir.getAbsolutePath());
							FileUtils.deleteDirectory(inputDir);
						}
					}
				}
			}
			for (int i = 0; i < pendingIOJobs; ++i) {
				printEcs.take();
			}
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			printExec.shutdown();
			// Wait until all threads are finish
			while (!printExec.isTerminated()) {
				Thread.sleep(5000);
				System.out.println((System.currentTimeMillis() - time)
						+ ": Shutting down");
			}

			app.countExec.shutdown();

			System.err.println(new Date() + ": Done in "
					+ (System.currentTimeMillis() - time) + " millis");
		} finally {
			try {
				notifyStream.close();
			} catch (IOException ignored) {

			}
			System.setErr(errOrig);
		}
	}

	public LoadCountsAsAttributes() throws IOException {
		shortColNameDict.load(FileUtils.openInputStream(FileUtils
				.getFile(shotColNamesPath)));

		countExec = Executors.newFixedThreadPool(Config.NUM_THREADS);
		countEcs = new ExecutorCompletionService<Instances>(countExec);

		// init the enumMap
		CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> loadDataFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder>();
		for (Object fileColnameObj : shortColNameDict.values()) {
			String fileColname = ((String) fileColnameObj);
			String fileName = fileColname
					.substring(0, fileColname.indexOf('_'));
			loadDataFactory.loadClass(LoadInputsIntoDocs.class, fileName);
		}
		// Set of features that appear only once per microloc. we can treat them
		// as nominal
		// rather than binary
		nominalAttrs = new HashSet<String>();
		if (!Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
			nominalAttrs.add(Config.RESULT_KEY_DURATION_FREQ);
			nominalAttrs.add(Config.RESULT_KEY_DAY_OF_WEEK_FREQ);
			nominalAttrs.add(Config.RESULT_KEY_HOUR_OF_DAY_FREQ);
			nominalAttrs.add(Config.RESULT_KEY_TEMPRATURE_FREQ);
			nominalAttrs.add(Config.RESULT_KEY_SKY_FREQ);
		}
		allAttributes = new FastVector();
		if (Config.LOAD_USE_PLACEID) {
			placeIdAttr = new Attribute("PlaceID", (FastVector) null);
			allAttributes.addElement(placeIdAttr);
		}
		valueDomainMap = Collections
				.synchronizedMap(new HashMap<String, HashMap<String, Attribute>>());

		for (Object statKeyShort : shortColNameDict.keySet()) {
			String statKey = (String) shortColNameDict.get(statKeyShort);
			HashMap<String, Attribute> valueDomain = new HashMap<String, Attribute>();
			valueDomainMap.put(statKey, valueDomain);
			if (Discretize.enumsMap.containsKey(statKey)) {
				if (nominalAttrs.contains(statKey)) {

					FastVector nominalVals = new FastVector();
					for (Enum<?> enumVal : Discretize.enumsMap.get(statKey)) {
						String enumStr = enumVal.toString();
						if (enumStr.equals(Config.MISSING_VALUE_PLACEHOLDER)) {
							continue;
						}
						nominalVals.addElement(enumStr);
					}

					Attribute attribute = new Attribute(
							statKeyShort.toString(), nominalVals);

					valueDomain.put(NOMINAL, attribute);
					allAttributes.addElement(attribute);
				} else {
					Enum<?>[] valueArray;
					if ((Config.QUANTIZE_NOT_DISCRETIZE || Config.WEKA_DISCRETIZE)
							&& Config.quantizedFields.containsKey(statKey)) {
						int end = Discretize.QuantilesEnum.values().length; // -
																			// 1;
						int start = 0;
						if (!Config.QUANTIZATION_PER_USER
								&& Config.DROP_LOWEST_QUANTILE) {
							++start;
						}
						if (!Config.QUANTIZATION_PER_USER
								&& Config.DROP_HIGHEST_QUANTILE) {
							--end;
						}

						valueArray = Arrays.copyOfRange(
								Discretize.QuantilesEnum.values(), start, end);
					} else {
						valueArray = Discretize.enumsMap.get(statKey);
					}
					for (Enum<?> enumVal : valueArray) {
						String enumStr = enumVal.toString();
						if (enumStr.equals(Config.MISSING_VALUE_PLACEHOLDER)) {
							continue;
						}
						Attribute attribute = new Attribute(
								statKeyShort.toString() + enumVal.toString());
						valueDomain.put(enumVal.toString(), attribute);
						allAttributes.addElement(attribute);
					}
				}
			} else {
				// The numeric attributes

				// log smoothing adds 1
				for (int i = 1; i <= MathUtil.POWS_OF_2.length; ++i) {
					Attribute attribute = new Attribute(statKeyShort.toString()
							+ (i));
					valueDomain.put(Integer.toString(i), attribute);
					allAttributes.addElement(attribute);
				}
				// System.out.println("Created numeric attribute for: " +
				// statKeyShort);
			}
		}
		FastVector labelsVector = new FastVector();
		for (String label : Config.LABELS_SINGLES) {
			labelsVector.addElement(label);
		}
		if (Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
			prevLabelAttributeArr = new Attribute[Config.LABELS_SINGLES.length];
			for (int i = 0; i < Config.LABELS_SINGLES.length; ++i) {
				prevLabelAttributeArr[i] = new Attribute("prevLabel"
						+ Config.LABELS_SINGLES[i]);
				allAttributes.addElement(prevLabelAttributeArr[i]);
			}
		} else {
			prevLabelAttribute = new Attribute("prev-label", labelsVector);
			allAttributes.addElement(prevLabelAttribute);
		}
		labelAttribute = new Attribute("label", labelsVector);

		// Make sure this is always the last label
		allAttributes.addElement(labelAttribute);

	}

	public ExecutorCompletionService<Instances> call() throws Exception {
		File dataDir = FileUtils.getFile(inputPath);

		for (File userDir : dataDir.listFiles()) {
			HashMap<String, HashMap<String, Integer>> appFreqMap = new HashMap<String, HashMap<String, Integer>>();
			for (File visit : userDir.listFiles()) {
				for (File microLFile : visit.listFiles()) {
					appFreqMap.put(microLFile.getName() + visit.getName(),
							new HashMap<String, Integer>());
				}
			}
			synchronized (allUsersAppFreqMap) {
				allUsersAppFreqMap.put(userDir.getName(), appFreqMap);
			}
			CountingCallable countCall = new CountingCallable(userDir,
					appFreqMap);
			countEcs.submit(countCall);
			++countingJobs;
			// Testing
			if (countingJobs == Config.NUM_USERS_TO_PROCESS)
				break;
		}
		return countEcs;
	}

	private static final Map<String, Frequency> userClassCount = Collections
			.synchronizedMap(new HashMap<String, Frequency>());

	private class CountingCallable implements Callable<Instances> {
		private final File userDir;
		private final HashMap<String, HashMap<String, Integer>> appFreqMap;

		public Instances call() throws Exception {
			String relName = userDir.getName();
			synchronized (userClassCount) {
				userClassCount.put(relName, new Frequency());
			}

			System.out.println((System.currentTimeMillis() - time)
					+ ": Reading user " + relName);

			Collection<File> microLocsFiles = FileUtils.listFiles(userDir,
					new String[] { "csv" }, true);
			weka.core.Instances wekaDoc = new weka.core.Instances(relName,
					allAttributes, microLocsFiles.size());
			wekaDoc.setClassIndex(labelAttribute.index());

			Long prevStartTime = null;
			Long prevEndTime = null;
			String prevLabel = null;
			String instLabel = null;
			String placeID = null;
			double instId = 1.0;
			Properties instIDPlaceID = new Properties();
			Writer instIDVisitTimeWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(OUTPUT_PATH, relName
									+ "_instid-time_map.properties"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				instIDVisitTimeWr
						.append("UserId\tInstId\tStartTime\tEndTime\n");
				for (File microLocF : microLocsFiles) {
					if (!Config.MICROLOC_SPLITS_DOCS) {
						if (microLocF.getName().endsWith(
								"" + Config.TIMETRUSTED_WLAN)) {
							throw new AssertionError("Splitted!!");
						}
					}

					Instance wekaInst;
					if (Config.LOAD_REPLACE_MISSING_VALUES) {
						wekaInst = new Instance(allAttributes.size());
					} else {
						wekaInst = new SparseInstance(allAttributes.size());
					}
					wekaInst.setDataset(wekaDoc);

					HashMap<Attribute, Integer> countMap = new HashMap<Attribute, Integer>();
					HashMap<String, Integer> featureCountMap = new HashMap<String, Integer>();
					HashMap<String, Integer> featureMaxMap = new HashMap<String, Integer>();
					HashMap<Attribute, String> attrPfxMap = new HashMap<Attribute, String>();
					for (int i = 0; i < allAttributes.size(); ++i) {
						countMap.put((Attribute) allAttributes.elementAt(i), 0);
					}

					Reader microLocR = Channels.newReader(FileUtils
							.openInputStream(microLocF).getChannel(),
							Config.OUT_CHARSET);
					int chInt;
					int numTabs = 0;
					StringBuffer header = new StringBuffer();

					long currStartTime = Long.parseLong(StringUtils
							.removeLastNChars(microLocF.getParentFile()
									.getName(), 1));
					long currEndTime = Long.parseLong(StringUtils
							.removeLastNChars(microLocF.getName(), 5));
					while ((chInt = microLocR.read()) != -1) {
						if ((char) chInt == '\t') {
							if (numTabs == 0) {
								// inst name.. not useful in weka
							} else if (numTabs == 1) {
								if (prevStartTime != null
										&& prevStartTime.equals(currStartTime)) {
									// same visit, different micro loc.. nothing
									// to
									// do
									// System.out.println("dummy!");
								} else if (prevEndTime != null
										&& currStartTime - prevEndTime < Config.INTERVAL_LABEL_CARRY_OVER) {
									prevLabel = instLabel;
								} else {
									prevLabel = null;
								}
								placeID = header.toString();
								instLabel = Config.placeLabels
										.getProperty(placeID);
							}
							header.setLength(0);
							++numTabs;
							if (numTabs == 2) {
								break;
							}
						} else {
							header.append((char) chInt);
						}
					}
					prevStartTime = currStartTime;
					prevEndTime = currEndTime;
					header = null;
					double visitLength = (currEndTime - currStartTime)
							/ Config.TIME_SECONDS_IN_10MINS * 1.0;

					if (Config.LOAD_USE_PLACEID) {
						wekaInst.setValue(0, placeID);
					}

					char[] dataChars = new char[Config.IO_BUFFER_SIZE];
					StringBuilder token = new StringBuilder();
					int len;
					while ((len = microLocR.read(dataChars)) > 0) {
						for (int i = 0; i < len; ++i) {
							if (Character.isLowerCase(dataChars[i])
							// Make sure this is not an integer valued feature
									|| (Character.isDigit(dataChars[i])
											&& token.indexOf("hod") == -1
											&& token.indexOf("apu") == -1
											&& token.indexOf("num") == -1
											&& token.indexOf("avg") == -1 && token
											.indexOf("sdv") == -1)) {
								token.append(dataChars[i]);

							} else {
								String pfx = token.toString();
								String statKey = shortColNameDict
										.getProperty(pfx);
								token.setLength(0);

								for (; i < dataChars.length; ++i) {
									if (Character.isWhitespace(dataChars[i])) {
										break;
									}
									token.append(dataChars[i]);
								}

								String value = token.toString().trim();
								token.setLength(0);

								// /
								if (value
										.endsWith(Config.MISSING_VALUE_PLACEHOLDER)) {
									// However this made it here, but it does
									// for
									// cgd and cgd2!!
									continue;
								}

								// It's now used to report the number of aps
								// being
								// used
								// if (pfx.equals("apu")) {
								// // redundant feature, because we now store
								// use
								// // of every app
								// continue;
								// }

								if (Config.LOAD_DROP_VERYFREQUENT_VALS
										&& (pfx.equals("si") && (value
												.equals("E") || value
												.equals("A")))) {
									// This is like a stop word that always
									// happens..
									// sure the user is actively using the
									// mobile!
									continue;
								}

								if (Config.LOAD_DROP_VERYFREQUENT_VALS
										&& (pfx.equals("aca") && (value
												.equals("E") || value
												.equals("S")))) {
									// This is like a stop word that always
									// happens..
									// sure the mobile is left laying down!
									continue;
								}

								HashMap<String, Attribute> valueDomain = valueDomainMap
										.get(statKey);
								Attribute attribute;
								if (valueDomain != null) {
									if (nominalAttrs.contains(statKey)) {
										// one value per microloc.. no counting
										attribute = valueDomain.get(NOMINAL);
										if (!value
												.equals(Config.MISSING_VALUE_PLACEHOLDER)) {
											wekaInst.setValue(attribute, value);
										} else if (Config.LOAD_REPLACE_MISSING_VALUES) {
											wekaInst.setValue(
													attribute,
													Config.LOAD_MISSING_VALUE_REPLA);
										}
										continue;
									} else {
										if (!Discretize.enumsMap
												.containsKey(statKey)) {
											// numeric attribute.. it must be
											// limited to
											// range
											double numVal = Double
													.parseDouble(value);

											if (pfx.startsWith("avg")
													|| pfx.startsWith("sdv")) {
												// numeric value that need
												// smoothing
												// TODO: Apply a weaker
												// smoothing...
												// the range isn't that high
												value = Long.toString(MathUtil
														.tf(numVal));
											}

											if (numVal < 1) {

												// negative or zero.. discard!
												// System.err.println("INFO: Discarding nonposetive token "
												// + pfx + " " + value);
												continue;
											} else if (numVal > MathUtil.POWS_OF_2.length - 1) {
												value = Integer
														.toString(MathUtil.POWS_OF_2.length - 1);
											}
										}
										attribute = valueDomain.get(value);
										if (attribute == null) {

											if (Config.MISSING_VALUE_PLACEHOLDER
													.equals(value)) {
												continue;
											} else if (!Config.QUANTIZATION_PER_USER) {
												if ((Config.DROP_HIGHEST_QUANTILE && value
														.equals(Discretize.QuantilesEnum
																.values()[Config.NUM_QUANTILES]
																.toString()))
														|| (Config.DROP_LOWEST_QUANTILE && value
																.equals(Discretize.QuantilesEnum
																		.values()[0]
																		.toString()))) {
													continue;
												}
											}
										}
									}
								} else if (pfx.equals("aid")) {
									String appName = featSelectedApps
											.getProperty(value);
									if (appName == null) {
										continue;
									}
									HashMap<String, Integer> appFreq = appFreqMap
											.get(microLocF.getName()
													+ microLocF.getParentFile()
															.getName());
									Integer appCnt = appFreq.get(value);
									if (appCnt == null) {
										appCnt = 0;
										// appFreq.put(value, appCnt);
									}
									++appCnt;
									appFreq.put(value, appCnt);
									continue;
								} else {
									if (!pfx.isEmpty()) {
										System.err
												.println("WARNING! You are loosing a token: "
														+ pfx + " " + value);

									}
									continue;
								}

								if (attribute == null) {
									System.err
											.println("Couldn't find attribute for pfx: "
													+ pfx
													+ " and value: "
													+ value
													+ " from file "
													+ microLocF
															.getAbsolutePath());
									continue;
								}

								Integer count = countMap.get(attribute);
								count = count + 1;
								countMap.put(attribute, count);

								if (!Config.LOAD_NORMALIZE_BY
										.equals(NORMALIZE_BY_ENUM.NONE)) {
									if (!attrPfxMap.containsKey(attribute)) {
										attrPfxMap.put(attribute, pfx);
									}
								}

								if (Config.LOAD_NORMALIZE_BY
										.equals(NORMALIZE_BY_ENUM.SUM)) {
									Integer featureCount = featureCountMap
											.get(pfx);
									if (featureCount == null) {
										featureCount = 0;
									}

									featureCountMap.put(pfx, featureCount + 1);
								} else if (Config.LOAD_NORMALIZE_BY
										.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
									Integer featureMax = featureMaxMap.get(pfx);
									if (featureMax == null
											|| count > featureMax) {
										featureMaxMap.put(pfx, count);
									}
								}
							}
						}
					}

					if (Config.LOAD_MISSING_CLASS_AS_OTHER && instLabel == null) {
						instLabel = Config.LABELS_SINGLES[0];
					}

					if (instLabel != null) {
						wekaInst.setClassValue(instLabel);
						synchronized (userClassCount) {
							userClassCount.get(relName).addValue(instLabel);
						}
					} else {
						wekaInst.setClassMissing();
					}

					if (Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
						// 1 means true, 0 flase.. or is missing better
						for (int i = 0; i < Config.LABELS_SINGLES.length; ++i) {
							double val = (Config.SPREAD_NOMINAL_FEATURES_USE_MISSING ? Double.NaN
									: 0);
							if (Config.LABELS_SINGLES[i].equals(prevLabel)) {
								val = 1.0;
							}
							wekaInst.setValue(prevLabelAttributeArr[i], val);
						}
					} else {
						if (prevLabel != null) {
							wekaInst.setValue(prevLabelAttribute, prevLabel);
						}
						// else if(Config.LOAD_REPLACE_MISSING_VALUES){
						// wekaInst.setValue(prevLabelAttribute,
						// Config.MISSING_VALUE_PLACEHOLDER);
						// }
					}

					for (Attribute attrib : countMap.keySet()) {
						if (attrib == labelAttribute
								|| attrib.name().contains("ID")) {
							continue;
						}
						if (Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
							int i = 0;
							for (; i < Config.LABELS_SINGLES.length; ++i) {
								if (prevLabelAttributeArr[i] == attrib) {
									break;
								}
							}
							if (i != Config.LABELS_SINGLES.length) {
								continue;
							}
						} else {
							if (attrib == prevLabelAttribute) {
								continue;
							}
						}

						int count = countMap.get(attrib);
						if (count > 0) {
							double normalizer = 1;
							if (Config.LOAD_NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.SUM)) {
								normalizer = featureCountMap.get(attrPfxMap
										.get(attrib));
							} else if (Config.LOAD_NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
								normalizer = featureMaxMap.get(attrPfxMap
										.get(attrib)); // TODO??: * 0.01; // so
														// that
														// the range is from 0
														// to
														// 100
							}
							if (Config.LOAD_NORMALIZE_VISIT_LENGTH) {
								normalizer *= visitLength;
							}
							wekaInst.setValue(attrib, count / normalizer);
						} else {
							if (Config.LOAD_REPLACE_MISSING_VALUES) {
								wekaInst.setValue(attrib,
										Config.LOAD_MISSING_VALUE_REPLA);
							} else {
								wekaInst.setMissing(attrib);
							}
						}

					}

					wekaDoc.add(wekaInst);

					instIDPlaceID.setProperty(Double.toString(instId), placeID);

					String startTime = StringUtils.removeLastNChars(microLocF
							.getParentFile().getName(), 1);
					String endTime = StringUtils.removeLastNChars(
							microLocF.getName(), 5);
					instIDVisitTimeWr.append(relName).append('\t')
							.append(Double.toString(instId)).append('\t')
							.append(startTime).append('\t').append(endTime)
							.append('\n');
					// .append("?").append('\t').append('\n');
					++instId;
				}
			} finally {
				instIDVisitTimeWr.flush();
				instIDVisitTimeWr.close();
			}
			if (instIDPlaceID.size() != wekaDoc.numInstances()) {
				throw new AssertionError(
						"There must be one record per instance that maps it to a visit!");
			}

			storeInstIdPlaceId(instIDPlaceID, relName);
			System.out.println((System.currentTimeMillis() - time)
					+ ": Finished reading user " + relName);
			return wekaDoc;
		}

		protected CountingCallable(File userDir,
				HashMap<String, HashMap<String, Integer>> appFreqMap) {
			// targetStats.clear();
			this.userDir = userDir;
			this.appFreqMap = appFreqMap;
		}
	}

	private static void storeInstIdPlaceId(Properties instIDPlaceID,
			String relName) throws IOException {
		Writer instIdPlaceIdWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(OUTPUT_PATH, relName
								+ "_instid-placeid_map.properties"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			instIDPlaceID.store(instIdPlaceIdWr, null);
		} finally {
			instIdPlaceIdWr.flush();
			instIdPlaceIdWr.close();
		}

	}

	static class InstancesTransformCallable implements Callable<Integer> {
		final Attribute placeIdAttr;
		private static final String PCA_CONCENSUS_REMOVE_FILTER = "249,248,247,246,245,244,243,241,238,232,227,219,218,217,216,215,214,199,184,183,182,181,180,179,176,175,174,167,166,165,164,163,159,158,157,156,155,154,151,150,149,148,147,139,138,137,136,135,134,133,132,131,130,129,128,127,126,125,124,123,122,121,120,119,118,117,114,113,112,108,107,103,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,81,80,79,78,76,75,74,73,72,70,69,68,67,66,64,63,62,58,57,56,54,53,52,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,9,8,7,6,4,3,2";
		// "249,248,247,246,245,244,243,241,238,232,227,219,218,217,216,215,214,199,184,183,182,181,180,179,176,175,174,167,166,165,164,163,159,158,157,156,155,154,151,150,149,148,147,139,138,137,136,135,134,133,132,131,130,129,128,127,126,125,124,123,122,121,120,119,118,117,114,113,112,108,107,103,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,81,80,79,78,76,75,74,73,72,70,69,68,67,66,64,63,62,58,57,56,54,53,52,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,9,8,7,6,4,3,2";
		private final File userDir;
		private final HashMap<String, HashMap<String, Integer>> appFreqMap;
		private final Pattern commaSplit = Pattern.compile("\\,");

		public InstancesTransformCallable(File userDir, Attribute placeIdAttr) {
			this.userDir = userDir;
			appFreqMap = allUsersAppFreqMap.get(userDir.getName());
			this.placeIdAttr = placeIdAttr;
		}

		@Override
		public Integer call() throws Exception {
			int printingJobs = 0;
			Instances userAppUsage = null;
			if (Config.LOADCOUNT_APP_USAGE) {
				userAppUsage = new Instances(userDir.getName(),
						appUsageAttrsFV, 0);
				for (File visit : userDir.listFiles()) {
					for (File microLoc : visit.listFiles()) {

						Instance microLocAppUsage = new Instance(
								appUsageAttrsFV.size());
						microLocAppUsage.setDataset(userAppUsage);
						HashMap<String, Integer> visitAppFreq = appFreqMap
								.get(microLoc.getName() + visit.getName());
						double normalizer = 1;
						if (Config.LOAD_NORMALIZE_BY
								.equals(NORMALIZE_BY_ENUM.SUM)) {
							for (String appUid : visitAppFreq.keySet()) {
								normalizer += visitAppFreq.get(appUid);
							}
						} else if (Config.LOAD_NORMALIZE_BY
								.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
							normalizer = Integer.MIN_VALUE;
							for (String appUid : visitAppFreq.keySet()) {
								double c = visitAppFreq.get(appUid);
								if (normalizer < c) {
									normalizer = visitAppFreq.get(appUid);
								}
							}
						}
						for (String appUid : appUsageAttrs.keySet()) {
							Attribute appAttr = appUsageAttrs.get(appUid);
							// if (appAttr == null) {
							if (!visitAppFreq.containsKey(appUid)) {
								if (Config.LOAD_REPLACE_MISSING_VALUES) {
									microLocAppUsage.setValue(appAttr,
											Config.LOAD_MISSING_VALUE_REPLA);
								} else {
									microLocAppUsage.setMissing(appAttr);
								}
								continue;
							}
							double appValue;
							if (Config.LOAD_NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.SUM)) {
								appValue = visitAppFreq.get(appUid)
										/ normalizer;
							} else if (Config.LOAD_NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
								appValue = MathUtil
										.tf(visitAppFreq.get(appUid))
										/ MathUtil.tf(normalizer);
							} else {
								appValue = MathUtil
										.tf(visitAppFreq.get(appUid));
							}
							if (Config.LOAD_NORMALIZE_VISIT_LENGTH) {
								appValue /= (Long
										.parseLong(StringUtils
												.removeLastNChars(
														microLoc.getName(), 5)) - Long
										.parseLong(StringUtils
												.removeLastNChars(
														visit.getName(), 1)));
							}
							microLocAppUsage.setValue(appAttr, appValue);
						}
						userAppUsage.add(microLocAppUsage);
					}
				}
			}
			if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {
				AppsSVMLightSaver saver = null;
				while ((saver = (AppsSVMLightSaver) AppsSVMLightSaver
						.acquireSaver(userAppUsage)) == null) {
					Thread.sleep(500);
				}
				try {
					printExec.submit(saver).get();
				} finally {
					saver.release();
				}
			} else {
				String userid;
				Instances joinedInsts;
				if (Config.LOADCOUNT_APP_USAGE) {
					userid = userAppUsage.relationName();

					Instances countsInsts = new Instances(Channels.newReader(
							FileUtils.openInputStream(
									FileUtils.getFile(TEMP_PATH, // Config.LABELS_MULTICLASS_NAME,
											userid + ".arff")).getChannel(),
							Config.OUT_CHARSET));
					joinedInsts = Instances.mergeInstances(userAppUsage,
							countsInsts);
					// joinedInsts.setClassIndex(joinedInsts.numAttributes() -
					// 1);
					joinedInsts.setRelationName(userid);
					countsInsts = null;
					userAppUsage = null;
				} else {
					userid = userDir.getName();
					joinedInsts = new Instances(Channels.newReader(FileUtils
							.openInputStream(FileUtils.getFile(TEMP_PATH, // Config.LABELS_MULTICLASS_NAME,
									userid + ".arff")).getChannel(),
							Config.OUT_CHARSET));
				}
				joinedInsts.setClassIndex(joinedInsts.numAttributes() - 1);
				// Treated as a feature, so first inst of each user has
				// something in common, .. etc
				// AddID addId = new AddID();
				// addId.setInputFormat(joinedInsts);
				// // addId.setIDIndex("first");
				// String idName = "ID";
				// addId.setAttributeName(idName);
				// joinedInsts = Filter.useFilter(joinedInsts, addId);

				if (Config.LOADCOUNTS_DELETE_MISSING_CLASS) {
					// Should have done this earlier
					// actually
					joinedInsts.deleteWithMissingClass();
				}

				joinedInsts.setRelationName(userid);

				System.out.println("Joined attrs for user: " + userid
						+ " has attributes: " + joinedInsts.numAttributes());

				Remove generalRemove = null;
				if (Config.LOAD_FEATSELECTED_ONLY) {

					generalRemove = new Remove();
					String remIxes = PCA_CONCENSUS_REMOVE_FILTER;
					// FileUtils.readFileToString(FileUtils
					// .getFile(AttributeConsensusRank.OUTPUT_PATH,
					// CLASSIFIER_TO_HONOUR, "filter_ALL.txt"));
					generalRemove.setInputFormat(joinedInsts);
					generalRemove.setAttributeIndices(remIxes);
					int numInstsBefore = joinedInsts.numAttributes();
					joinedInsts = Filter.useFilter(joinedInsts, generalRemove);
					if (numInstsBefore == joinedInsts.numAttributes()) {
						for (String attrIxStr : commaSplit.split(remIxes)) {
							joinedInsts.deleteAttributeAt(Integer
									.parseInt(attrIxStr) - 1);
						}
					}
				}

				joinedInsts.setRelationName(userid);

				int placeIdAttrIx = -1;
				if (Config.LOAD_USE_PLACEID) {
					placeIdAttrIx = searchForAttr(joinedInsts, placeIdAttr);
				}

				printEcs.submit(new ArffSaverCallable(joinedInsts,
						FilenameUtils.concat(OUTPUT_PATH,
								Config.LABELS_MULTICLASS_NAME),
						!Config.LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER, false,
						placeIdAttrIx));

				++printingJobs;

				for (String positiveClass : Config.LABEL_HIERARCHY) {
					int existingPositive = 0;
					int existingNEgative = 0;
					for (String someClass : Config.LABELS_SINGLES) {
						int clsIx = positiveClass.indexOf(someClass);
						Boolean clsPositive = null;
						if (positiveClass.charAt(clsIx + someClass.length()) == '+') {
							clsPositive = true;
						} else if (positiveClass.charAt(clsIx
								+ someClass.length()) == '-') {
							clsPositive = false;
						}
						if (clsPositive == null) {
							continue; // 1 digit class trapped in a 2 digit
										// positiveclas
						}
						long numForUser;
						synchronized (userClassCount) {
							numForUser = userClassCount.get(userid).getCount(
									someClass);
						}
						if (clsPositive) {
							existingPositive += numForUser;
						} else {
							existingNEgative += numForUser;
						}
					}
					FileUtils.writeStringToFile(
							FileUtils.getFile(
									FilenameUtils.concat(OUTPUT_PATH, "c"
											+ positiveClass), userid
											+ "-counts.txt"), "Positive: "
									+ existingPositive + " - Negative: "
									+ existingNEgative);

					if (existingNEgative == 0 // || OR throws away positive
							// samples, wrong??
							&& existingPositive == 0) {
						// if(existingPositive == 0){
						// not a usefule user for this positive class
						System.out
								.println(userid + " is not useful for classes "
										+ positiveClass);
						continue;
					}

					Instances copyInsts = new Instances(joinedInsts);
					if (Config.LOAD_FEATSELECTED_ONLY
							&& Config.LOAD_USER_SPECIFIC_FEATURE_FILTER) {

						File filterFile = FileUtils
								.getFile(
										AttributeConsensusRank.OUTPUT_PATH,
										CLASSIFIER_TO_HONOUR,
										ATRRSELECTOR_TO_HONOUR,
										"filter"
												+ positiveClass
												+ AttributeConsensusRank.FEAT_SELECTION_FNAME_SUFFIX);

						if (filterFile.exists()) {
							String remIxes = FileUtils
									.readFileToString(filterFile);
							Remove remove = new Remove();
							remove.setInputFormat(copyInsts);
							remove.setAttributeIndices(remIxes);
							copyInsts = Filter.useFilter(copyInsts, remove);

						} else {

							System.out.println("No filter for positiveclass: "
									+ positiveClass);
							copyInsts = Filter.useFilter(copyInsts,
									generalRemove);
						}
					}

					// Prepare for all cases
					// if (Config.CLASSIFY_USING_BIANRY_ENSEMBLE) {

					Add add = new Add();
					add.setAttributeIndex("last");
					add.setAttributeName("binary-label");
					add.setNominalLabels(Config.LABELS_BINARY[0] + ","
							+ Config.LABELS_BINARY[1]);
					add.setInputFormat(copyInsts);

					copyInsts = Filter.useFilter(copyInsts, add);

					// AddID addId = new AddID();
					// addId.setInputFormat(copyInsts);
					// // addId.setIDIndex("first");
					// String idName = "ID";
					// addId.setAttributeName(idName);
					// copyInsts = Filter.useFilter(copyInsts, addId);

					Writer trueLableWr = Channels
							.newWriter(
									FileUtils
											.openOutputStream(
													FileUtils.getFile(
															FilenameUtils
																	.concat(OUTPUT_PATH,
																			"c"
																					+ positiveClass),
															userid
																	+ "_actual-labels.properties"))
											.getChannel(), Config.OUT_CHARSET);

					try {
						@SuppressWarnings("rawtypes")
						Enumeration instEnum = copyInsts.enumerateInstances();
						double idVal = 0.0;
						while (instEnum.hasMoreElements()) {
							Instance copyInst = (Instance) instEnum
									.nextElement();

							String cls = Long.toString(Math.round(copyInst
									.classValue()));

							// long idVal = Math.round(copyInst.value(0));
							// trueLableWr.append(Long.toString(idVal))
							// double idVal = copyInst.value(0);
							++idVal;
							trueLableWr.append(Double.toString(idVal))
									.append("=").append(cls).append('\n');

							String binaryLabel = null;
							if (positiveClass.contains("+" + cls + "+")) {
								binaryLabel = "+1";

							} else if (positiveClass.contains("-" + cls + "-")) {
								binaryLabel = "-1";
							}

							if (binaryLabel != null) {
								copyInst.setValue(copyInst.numAttributes() - 1,
										binaryLabel);
							} else {
								copyInst.setMissing(copyInst.numAttributes() - 1);
							}

							copyInst.setDataset(copyInsts);

						}
					} finally {
						trueLableWr.flush();
						trueLableWr.close();
					}

					// Remove the multinomial class
					Remove rem = new Remove();
					// The index range starts from 1 here
					rem.setAttributeIndices(Integer.toString(copyInsts
							.numAttributes() - 1));
					rem.setInputFormat(copyInsts);
					copyInsts = Filter.useFilter(copyInsts, rem);

					copyInsts.setClassIndex(copyInsts.numAttributes() - 1);

					if (!Config.LOADCOUNTS_FOR_SVMLIGHT_TRANSDUCTIVE) {
						copyInsts.deleteWithMissingClass();
					}

					copyInsts.setRelationName(userid);

					int copyPlaceIdAttrIx = placeIdAttrIx;
					// searchForAttr(copyInsts,placeIdAttr);

					printEcs.submit(new ArffSaverCallable(copyInsts,
							FilenameUtils.concat(OUTPUT_PATH, "c"
									+ positiveClass),
							!Config.LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER, false,
							copyPlaceIdAttrIx));
					++printingJobs;
				}

				// ArffSaverCallable arffSaveCall = new ArffSaverCallable(
				// userAppUsage, Config.LABELS_MULTICLASS_NAME, ".app");
				// // arffSaveCall.outPath = FilenameUtils
				// // .removeExtension(arffSaveCall.outPath);
				// // arffSaveCall.outPath += ".app";
				// printExec.submit(arffSaveCall);
			}
			return printingJobs;
		}

		private int searchForAttr(Instances joinedInsts, Attribute attr) {
			if (attr == null) {
				return -1;
			}
			int result = 0;
			Enumeration attrEnum = joinedInsts.enumerateAttributes();
			while (attrEnum.hasMoreElements()) {
				if (attr.equals((Attribute) attrEnum.nextElement())) {
					return result;
				} else {
					++result;
				}
			}
			return -1;
		}

	}
}
