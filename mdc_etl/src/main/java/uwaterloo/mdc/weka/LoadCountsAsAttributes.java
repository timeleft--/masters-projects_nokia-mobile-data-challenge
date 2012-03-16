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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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
import weka.core.converters.ArffSaver;

//import uwaterloo.mdc.etl.mallet.*;

public class LoadCountsAsAttributes implements
		Callable<ExecutorCompletionService<Instances>> {

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
				result = Long.toString(Math.round(currInst.classValue() + 1));
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
//			if (result == null) {
//				throw new IndexOutOfBoundsException(
//						"The files of the apps and the other input should be the same length");
//			}
			return result;
		}
	}

	private static class ArffSaverCallable implements Callable<Void> {
		String outPath = "C:\\mdc-datasets\\weka\\segmented_user";
		Instances insts;

		public ArffSaverCallable(Instances insts) {
			this.insts = insts;
			outPath = FilenameUtils.concat(outPath, insts.relationName()
					+ ".arff");
		}

		@Override
		public Void call() throws Exception {
			File dest = FileUtils.getFile(outPath);
			System.out.println((System.currentTimeMillis() - time)
					+ ": Writing " + dest.getAbsolutePath());

			ArffSaver arffsaver = new ArffSaver();
			arffsaver.setInstances(insts);
			arffsaver.setDestination(FileUtils.openOutputStream(dest));
			// arffsaver.setCompressOutput(true);
			arffsaver.writeBatch();
			System.out.println((System.currentTimeMillis() - time)
					+ ": Finished writing " + dest.getAbsolutePath());

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

	private Map<String, HashMap<String, Attribute>> valueDomainMap;
	private FastVector allAttributes;
	private HashSet<String> nominalAttrs;
	private Attribute labelAttribute;
	private Attribute prevLabelAttribute;
	private Attribute[] prevLabelAttributeArr;
	private int countingJobs;

	private HashMap<String, HashMap<String, HashMap<String, Integer>>> allUsersAppFreqMap = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();

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

		LoadCountsAsAttributes app = new LoadCountsAsAttributes();

		if (Config.LOADCOUNTS_FOR_SVMLIGHT) {
			printExec = Executors.newSingleThreadExecutor();
			SVMLightSaverSingleton.init(app.inputPath,
					Config.NUM_USERS_TO_PROCESS); // TODO: support more than LOO
		} else {
			printExec = Executors.newFixedThreadPool(Config.NUM_THREADS / 2);
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

			if (Config.LOADCOUNTS_FOR_SVMLIGHT) {
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
				printExec.submit(new ArffSaverCallable(insts));
				++printingJobs;
			}
		}

		if (Config.LOADCOUNTS_FOR_SVMLIGHT) {

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
			HashMap<String, HashMap<String, Integer>> appFreqMap = app.allUsersAppFreqMap
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

		HashMap<String, Attribute> appUsageAttrs = new HashMap<String, Attribute>();
		FastVector appUsageAttrsFV = new FastVector();
		for (String appUid : aggregateAppFreq.keySet()) {
			Integer appUsageOccurs = aggregateAppFreq.get(appUid);
			if (appUsageOccurs > stopWordAppTh
					|| appUsageOccurs < rareWordAppTh) {
				continue;
			}
			Attribute appAttr = new Attribute(appUid);
			appUsageAttrs.put(appUid, appAttr);
			appUsageAttrsFV.addElement(appAttr);
		}

		if (Config.LOADCOUNTS_FOR_SVMLIGHT) {
			// TODO support more than LOO
			AppsSVMLightSaver.init(app.inputPath, Config.SVMLIGHT_INPUTPATH,
					Config.NUM_USERS_TO_PROCESS, app.allAttributes.size());
		}

		userIx = 0;
		for (File userDir : dataDir.listFiles()) {
			if (userIx == Config.NUM_USERS_TO_PROCESS) {
				break;
			}
			HashMap<String, HashMap<String, Integer>> appFreqMap = app.allUsersAppFreqMap
					.get(userDir.getName());
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
					if (Config.NORMALIZE_BY.equals(NORMALIZE_BY_ENUM.SUM)) {
						for (String appUid : visitAppFreq.keySet()) {
							normalizer += visitAppFreq.get(appUid);
						}
					} else if (Config.NORMALIZE_BY
							.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
						normalizer = Integer.MIN_VALUE;
						for (String appUid : visitAppFreq.keySet()) {
							double c = visitAppFreq.get(appUid);
							if (normalizer < c) {
								normalizer = visitAppFreq.get(appUid);
							}
						}
					}
					for (String appUid : visitAppFreq.keySet()) {
						Attribute appAttr = appUsageAttrs.get(appUid);
						if (appAttr == null) {
							continue;
						}
						if (Config.NORMALIZE_BY.equals(NORMALIZE_BY_ENUM.SUM)) {
							microLocAppUsage.setValue(appAttr,
									visitAppFreq.get(appUid) / normalizer);
						} else if (Config.NORMALIZE_BY
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

			if (Config.LOADCOUNTS_FOR_SVMLIGHT) {
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
				ArffSaverCallable arffSaveCall = new ArffSaverCallable(
						userAppUsage);
				arffSaveCall.outPath = FilenameUtils
						.removeExtension(arffSaveCall.outPath);
				arffSaveCall.outPath += ".app";
				printExec.submit(arffSaveCall);
			}
			++userIx;
		}

		if (Config.LOADCOUNTS_FOR_SVMLIGHT) {

			AppsSVMLightSaver noncontended = (AppsSVMLightSaver) AppsSVMLightSaver
					.acquireSaver(null);
			noncontended.dispose();
			noncontended.release();

			File outputDir = FileUtils.getFile(Config.SVMLIGHT_INPUTPATH);
			for (File srcDir : outputDir.listFiles()) {
				for (File srcFile : FileUtils.listFiles(srcDir,
						new String[] { "csv" }, false)) {
					for (int i = 1; i <= 10; ++i) { // classes
						String positiveClass = Integer.toString(i);
						Writer destWr = Channels.newWriter(
								FileUtils.openOutputStream(
										FileUtils.getFile(srcDir, "c"
												+ positiveClass, srcFile.getName()))
										.getChannel(), Config.OUT_CHARSET);
						try {
							BufferedReader srcRead = new BufferedReader(
									Channels.newReader(FileUtils
											.openInputStream(srcFile)
											.getChannel(), Config.OUT_CHARSET));
							String line;
							while ((line = srcRead.readLine()) != null) {
								if (line.startsWith(positiveClass)) {
									destWr.append("+1").append(
											line.substring(positiveClass
													.length())).append(System.lineSeparator());
								} else if (line.startsWith("0")) {
									if(Config.LOADCOUNTS_FOR_SVMLIGHT_TRANSDUCTIVE){
										destWr.append(line).append(System.lineSeparator());
									}
								} else {
									destWr.append("-1").append(
											line.substring(line.indexOf(' '))).append(System.lineSeparator());
								}
							}
						} finally {
							destWr.flush();
							destWr.close();
						}

						// Also prepare files for SVMlight output.. it needs a
						// file
						// to overwrite!
						for (int t = 0; t < 4; ++t) {
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											srcDir.getName(), "c" + i, "t" + t,
											"alpha.txt")).close();
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											srcDir.getName(), "c" + i, "t" + t,
											"model.txt")).close();
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											srcDir.getName(), "c" + i, "t" + t,
											"trans.txt")).close();
							FileUtils.openOutputStream(
									FileUtils.getFile(
											Config.SVMLIGHT_OUTPUTPATH,
											srcDir.getName(), "c" + i, "t" + t,
											"predictions.txt")).close();
						}
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

		countExec = Executors.newFixedThreadPool(Config.NUM_THREADS / 2);
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
					if (Config.QUANTIZE_NOT_DISCRETIZE
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
		for (String label : Config.LABELS) {
			labelsVector.addElement(label);
		}
		if (Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
			prevLabelAttributeArr = new Attribute[Config.LABELS.length];
			for (int i = 0; i < Config.LABELS.length; ++i) {
				prevLabelAttributeArr[i] = new Attribute("prevLabel"
						+ Config.LABELS[i]);
				allAttributes.addElement(prevLabelAttributeArr[i]);
			}
		} else {
			prevLabelAttribute = new Attribute("prevLabel", labelsVector);
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
			allUsersAppFreqMap.put(userDir.getName(), appFreqMap);
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

	private class CountingCallable implements Callable<Instances> {
		private final File userDir;
		private final HashMap<String, HashMap<String, Integer>> appFreqMap;

		public Instances call() throws Exception {

			System.out.println((System.currentTimeMillis() - time)
					+ ": Reading user " + userDir.getName());

			Collection<File> microLocsFiles = FileUtils.listFiles(userDir,
					new String[] { "csv" }, true);
			weka.core.Instances wekaDoc = new weka.core.Instances(
					userDir.getName(), allAttributes, microLocsFiles.size());
			wekaDoc.setClassIndex(labelAttribute.index());

			Long prevStartTime = null;
			Long prevEndTime = null;
			String prevLabel = null;
			String instLabel = null;
			for (File microLocF : microLocsFiles) {
				weka.core.Instance wekaInst = new weka.core.Instance(
						allAttributes.size());
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
				while (microLocR.read(dataChars) > 0) {
					for (int i = 0; i < dataChars.length; ++i) {
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

							if (pfx.equals("apu")) {
								// redundant feature, because we now store use
								// of every app
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

							Integer count = countMap.get(attribute);
							count = count + 1;
							countMap.put(attribute, count);

							if (!Config.NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.NONE)) {
								if (!attrPfxMap.containsKey(attribute)) {
									attrPfxMap.put(attribute, pfx);
								}
							}

							if (Config.NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.SUM)) {
								Integer featureCount = featureCountMap.get(pfx);
								if (featureCount == null) {
									featureCount = 0;
								}

								featureCountMap.put(pfx, featureCount + 1);
							} else if (Config.NORMALIZE_BY
									.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
								Integer featureMax = featureMaxMap.get(pfx);
								if (featureMax == null || count > featureMax) {
									featureMaxMap.put(pfx, count);
								}
							}
						}
					}
				}

				if (instLabel != null) {
					wekaInst.setClassValue(instLabel);
				} else {
					wekaInst.setClassMissing();
				}

				if (prevLabel != null) {
					if (Config.SPREAD_NOMINAL_FEATURES_AS_BINARY) {
						int i = 0;
						for (; i < Config.LABELS.length; ++i) {
							if (Config.LABELS[i].equals(prevLabel)) {
								break;
							}
						}
						wekaInst.setValue(prevLabelAttributeArr[i], 1);
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
						for (; i < Config.LABELS.length; ++i) {
							if (prevLabelAttributeArr[i] == attrib) {
								break;
							}
						}
						if (i != Config.LABELS.length) {
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
						if (Config.NORMALIZE_BY.equals(NORMALIZE_BY_ENUM.SUM)) {
							normalizer = featureCountMap.get(attrPfxMap
									.get(attrib));
						} else if (Config.NORMALIZE_BY
								.equals(NORMALIZE_BY_ENUM.MAXIMUM)) {
							normalizer = featureMaxMap.get(attrPfxMap
									.get(attrib)); // TODO??: * 0.01; // so that
													// the range is from 0 to
													// 100
						}
						wekaInst.setValue(attrib, count / normalizer);
					} else {
						wekaInst.setMissing(attrib);
					}

				}

				wekaDoc.add(wekaInst);
			}
			System.out.println((System.currentTimeMillis() - time)
					+ ": Finished reading user " + userDir.getName());
			return wekaDoc;
		}

		protected CountingCallable(File userDir,
				HashMap<String, HashMap<String, Integer>> appFreqMap) {
			// targetStats.clear();
			this.userDir = userDir;
			this.appFreqMap = appFreqMap;
		}
	}

}
