package uwaterloo.mdc.weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Config.NORMALIZE_BY_ENUM;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.mallet.LoadInputsIntoDocs;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.converters.ArffSaver;
import weka.core.converters.SVMLightSaver;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.AddID;
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

	public static final String OUTPUT_PATH = "C:\\mdc-datasets\\weka\\segmented_user";
	public static final String TEMP_PATH = "C:\\mdc-datasets\\weka\\segmented_user_temp";

	private static class ArffSaverCallable implements Callable<Void> {
		final String outPath;
		Instances insts;
		private boolean arffFmt = true;

		public ArffSaverCallable(Instances insts, String outPath,
				boolean arffFormat) {
			this(insts, outPath);
			arffFmt = arffFormat;
		}

		public ArffSaverCallable(Instances insts, String outPath) {
			this.insts = insts;
			this.outPath = FilenameUtils.concat(outPath,
			// FilenameUtils.concat(BASE_ARFFS_PATH, outPath),
					insts.relationName());
		}

		@Override
		public Void call() throws Exception {
			File dest = FileUtils.getFile(outPath
					+ (arffFmt ? ".arff" : ".csv"));
			System.out.println((System.currentTimeMillis() - time)
					+ ": Writing " + dest.getAbsolutePath());

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
			// System.out.println((System.currentTimeMillis() - time)
			// + ": Finished writing " + dest.getAbsolutePath());

			return null;
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
	// private static ExecutorCompletionService<Void> printEcs;

	private static Properties featSelectedApps;
	private static FastVector appUsageAttrsFV;
	private static final Map<String, Attribute> appUsageAttrs = Collections
			.synchronizedMap(new HashMap<String, Attribute>());

	private Map<String, HashMap<String, Attribute>> valueDomainMap;
	private FastVector allAttributes;
	private HashSet<String> nominalAttrs;
	private Attribute labelAttribute;
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
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));
		Config.quantizedFields = new Properties();
		Config.quantizedFields.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));

		featSelectedApps = new Properties();
		featSelectedApps.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.FEAT_SELECTED_APPS_PATH)));

		LoadCountsAsAttributes app = new LoadCountsAsAttributes();

		if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {
			printExec = Executors.newSingleThreadExecutor();
			SVMLightSaverSingleton.init(app.inputPath,
					Config.NUM_USERS_TO_PROCESS); // TODO: support more than LOO
		} else {
			printExec = Executors.newFixedThreadPool(Config.NUM_THREADS);
		}

		ExecutorCompletionService<Instances> ecs = app.call();
		int printingJobs = 0;
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
				printExec.submit(new ArffSaverCallable(insts, TEMP_PATH));
				++printingJobs;

			}
		}

		if (Config.LOADCOUNTS_FOR_SVMLIGHT_MY_CODE) {

			SVMLightSaverSingleton noncontended = SVMLightSaverSingleton
					.acquireSaver(null);
			noncontended.dispose();
			noncontended.release();
		} else {
			if (printingJobs != app.countingJobs) {
				System.err.println("You printed only " + printingJobs
						+ " ARFFs, you needed: " + app.countingJobs);
			}
		}
		// Handle App counts
		int maxAppUsageOccurs = Integer.MIN_VALUE;
		HashMap<String, Integer> aggregateAppFreq = new HashMap<String, Integer>();
		File dataDir = FileUtils.getFile(app.inputPath);
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
						int newFreq = appCnt + microLocAppFreq.get(appUid);
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
		double[] aggregateAppFreqVals = new double[aggregateAppFreq.size()];
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
			AppsSVMLightSaver.init(app.inputPath, Config.SVMLIGHT_INPUTPATH,
					Config.NUM_USERS_TO_PROCESS, app.allAttributes.size());
		}

		LinkedList<Future<Integer>> transformFutures = new LinkedList<Future<Integer>>();
		ExecutorService tranformExec = Executors
				.newFixedThreadPool(Config.NUM_THREADS);
		userIx = 0;
		for (File userDir : dataDir.listFiles()) {
			if (userIx == Config.NUM_USERS_TO_PROCESS) {
				break;
			}

			transformFutures.add(tranformExec
					.submit(new InstancesTransformCallable(userDir)));

			++userIx;
		}

		for (Future<Integer> addedPrintTasks : transformFutures) {
			printingJobs += addedPrintTasks.get();
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
					File inputDir = FileUtils.getFile(srcDir.getParentFile(),
							"c" + positiveClass, srcDir.getName());
					Writer destWr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(inputDir,
											srcFile.getName())).getChannel(),
							Config.OUT_CHARSET);
					try {
						BufferedReader srcRead = new BufferedReader(
								Channels.newReader(
										FileUtils.openInputStream(srcFile)
												.getChannel(),
										Config.OUT_CHARSET));
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
													+ srcFile.getAbsolutePath());
								}
								++positiveExamples;

							} else if (line.startsWith("0")) {
								if (Config.LOADCOUNTS_FOR_SVMLIGHT_TRANSDUCTIVE) {
									destWr.append(line).append(
											System.lineSeparator());
								}
							} else if (positiveClass.contains("-" + cls + "-")) {
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
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											outDirPathPfx + t, "alpha.txt"))
									.close();
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											outDirPathPfx + t, "model.txt"))
									.close();
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											outDirPathPfx + t, "trans.txt"))
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
		// for (int i = 0; i < printingJobs; ++i) {
		// printEcs.take();
		// }
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

		System.out.println(new Date() + ": Done in "
				+ (System.currentTimeMillis() - time) + " millis");

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
			for (File microLocF : microLocsFiles) {
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
						.removeLastNChars(microLocF.getParentFile().getName(),
								1));
				long currEndTime = Long.parseLong(StringUtils.removeLastNChars(
						microLocF.getName(), 5));
				while ((chInt = microLocR.read()) != -1) {
					if ((char) chInt == '\t') {
						if (numTabs == 0) {
							// inst name.. not useful in weka
						} else if (numTabs == 1) {
							if (prevStartTime != null
									&& prevStartTime.equals(currStartTime)) {
								// same visit, different micro loc.. nothing to
								// do
								// System.out.println("dummy!");
							} else if (prevEndTime != null
									&& currStartTime - prevEndTime < Config.INTERVAL_LABEL_CARRY_OVER) {
								prevLabel = instLabel;
							} else {
								prevLabel = null;
							}
							instLabel = Config.placeLabels.getProperty(header
									.toString());
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
							String statKey = shortColNameDict.getProperty(pfx);
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
								// However this made it here, but it does for
								// cgd and cgd2!!
								continue;
							}

							if (pfx.equals("apu")) {
								// redundant feature, because we now store use
								// of every app
								continue;
							}

							if (Config.LOAD_DROP_VERYFREQUENT_VALS && (pfx.equals("si")
									&& (value.equals("E") || value.equals("A")))) {
								// This is like a stop word that always
								// happens..
								// sure the user is actively using the mobile!
								continue;
							}

							if (Config.LOAD_DROP_VERYFREQUENT_VALS && (pfx.equals("aca")
									&& (value.equals("E") || value.equals("S")))) {
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
										wekaInst.setValue(attribute,
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
											// numeric value that need smoothing
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
												+ microLocF.getAbsolutePath());
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
								Integer featureCount = featureCountMap.get(pfx);
								if (featureCount == null) {
									featureCount = 0;
								}

								featureCountMap.put(pfx, featureCount + 1);
							} else if (Config.LOAD_NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
								Integer featureMax = featureMaxMap.get(pfx);
								if (featureMax == null || count > featureMax) {
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

				if (prevLabel != null) {
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
						wekaInst.setValue(prevLabelAttribute, prevLabel);
					}
				}

				for (Attribute attrib : countMap.keySet()) {
					if (attrib == labelAttribute) {
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
									.get(attrib)); // TODO??: * 0.01; // so that
													// the range is from 0 to
													// 100
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
			}
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

	static class InstancesTransformCallable implements Callable<Integer> {

		private final File userDir;
		private final HashMap<String, HashMap<String, Integer>> appFreqMap;

		public InstancesTransformCallable(File userDir) {
			this.userDir = userDir;
			appFreqMap = allUsersAppFreqMap.get(userDir.getName());
		}

		@Override
		public Integer call() throws Exception {

			int printingJobs = 0;
			Instances userAppUsage = new Instances(userDir.getName(),
					appUsageAttrsFV, 0);
			for (File visit : userDir.listFiles()) {
				for (File microLoc : visit.listFiles()) {
					Instance microLocAppUsage = new Instance(
							appUsageAttrsFV.size());
					microLocAppUsage.setDataset(userAppUsage);
					HashMap<String, Integer> visitAppFreq = appFreqMap
							.get(microLoc.getName() + visit.getName());
					double normalizer = 1;
					if (Config.LOAD_NORMALIZE_BY.equals(NORMALIZE_BY_ENUM.SUM)) {
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
						if (Config.LOAD_NORMALIZE_BY
								.equals(NORMALIZE_BY_ENUM.SUM)) {
							microLocAppUsage.setValue(appAttr,
									visitAppFreq.get(appUid) / normalizer);
						} else if (Config.LOAD_NORMALIZE_BY
								.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
							microLocAppUsage.setValue(appAttr,
									MathUtil.tf(visitAppFreq.get(appUid))
											/ MathUtil.tf(normalizer));
						} else {
							microLocAppUsage.setValue(appAttr,
									MathUtil.tf(visitAppFreq.get(appUid)));
						}
					}
					userAppUsage.add(microLocAppUsage);
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

				String userid = userAppUsage.relationName();

				Instances countsInsts = new Instances(Channels.newReader(
						FileUtils.openInputStream(FileUtils.getFile(TEMP_PATH, // Config.LABELS_MULTICLASS_NAME,
								userid + ".arff")).getChannel(),
						Config.OUT_CHARSET));
				Instances joinedInsts = Instances.mergeInstances(userAppUsage,
						countsInsts);
				joinedInsts.setClassIndex(joinedInsts.numAttributes() - 1);
				joinedInsts.setRelationName(userid);
				countsInsts = null;
				userAppUsage = null;

				if (Config.LOADCOUNTS_DELETE_MISSING_CLASS) {
					// Should have done this earlier
					// actually
					joinedInsts.deleteWithMissingClass();
				}
				
				AddID addId = new AddID();
				addId.setInputFormat(joinedInsts);
				// addId.setIDIndex("first");
				String idName = "ID";
				addId.setAttributeName(idName);
				joinedInsts = Filter.useFilter(joinedInsts, addId);

				joinedInsts.setRelationName(userid);
				
				Instances copyInsts = joinedInsts;
				Remove generalRemove;
				if (Config.LOAD_FEATSELECTED_ONLY) {

					generalRemove = new Remove();
					String remIxes = FileUtils.readFileToString(FileUtils
							.getFile(AttributeConsensusRank.OUTPUT_PATH,
									CLASSIFIER_TO_HONOUR, "filter_ALL.txt"));
					generalRemove = new Remove();
					generalRemove.setInputFormat(copyInsts);
					generalRemove.setAttributeIndices(remIxes);
					copyInsts = Filter.useFilter(copyInsts, generalRemove);
				}

				copyInsts.setRelationName(joinedInsts.relationName());

				printExec.submit(new ArffSaverCallable(copyInsts, FilenameUtils
						.concat(OUTPUT_PATH, Config.LABELS_MULTICLASS_NAME),
						!Config.LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER));

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
					if (existingNEgative == 0 || existingPositive == 0) {
						// not a usefule user for this positive class
						System.out
								.println(userid + " is not useful for classes "
										+ positiveClass);
						continue;
					}

					FileUtils.writeStringToFile(
							FileUtils.getFile(
									FilenameUtils.concat(OUTPUT_PATH, "c"
											+ positiveClass), userid
											+ "-counts.txt"), "Positive: "
									+ existingPositive + " - Negative: "
									+ existingNEgative);

					copyInsts = joinedInsts;
					if (Config.LOAD_FEATSELECTED_ONLY) {

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
					add.setInputFormat(joinedInsts);

					copyInsts = new Instances(joinedInsts);

					copyInsts = Filter.useFilter(copyInsts, add);

//					AddID addId = new AddID();
//					addId.setInputFormat(copyInsts);
//					// addId.setIDIndex("first");
//					String idName = "ID";
//					addId.setAttributeName(idName);
//					copyInsts = Filter.useFilter(copyInsts, addId);

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

						while (instEnum.hasMoreElements()) {
							Instance copyInst = (Instance) instEnum
									.nextElement();

							String cls = Long.toString(Math.round(copyInst
									.classValue()));

							long idVal = Math.round(copyInst.value(0));
							trueLableWr.append(Long.toString(idVal))
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
					// Will never happen, and we want to fix the ID
					// copyInsts.deleteWithMissingClass();
					// }

					copyInsts.setRelationName(joinedInsts.relationName());

					printExec.submit(new ArffSaverCallable(copyInsts,
							FilenameUtils.concat(OUTPUT_PATH, "c"
									+ positiveClass),
							!Config.LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER));
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

	}
}
