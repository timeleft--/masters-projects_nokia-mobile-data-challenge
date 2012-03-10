package uwaterloo.mdc.weka;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
	private Attribute labelAttribute;
	private Attribute prevLabelAttribute;
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

		printExec = Executors.newFixedThreadPool(Config.NUM_THREADS / 2);

		LoadCountsAsAttributes app = new LoadCountsAsAttributes();

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

			printExec.submit(new ArffSaverCallable(insts));
			++printingJobs;
		}

		if (printingJobs != app.countingJobs) {
			System.err.println("You printed only " + printingJobs
					+ " ARFFs, you needed: " + app.countingJobs);
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
					Instance microLocAppUsage = new Instance(appUsageAttrsFV.size());
					microLocAppUsage.setDataset(userAppUsage);
					HashMap<String, Integer> visitAppFreq = appFreqMap
							.get(microLoc.getName() + visit.getName());
					for (String appUid : visitAppFreq.keySet()) {
						Attribute appAttr = appUsageAttrs.get(appUid);
						if (appAttr == null) {
							continue;
						}
						microLocAppUsage.setValue(appAttr,
								MathUtil.tf(visitAppFreq.get(appUid)));
					}
					userAppUsage.add(microLocAppUsage);
				}
			}
			ArffSaverCallable arffSaveCall = new ArffSaverCallable(userAppUsage);
			arffSaveCall.outPath = FilenameUtils
					.removeExtension(arffSaveCall.outPath);
			arffSaveCall.outPath += ".app";
			printExec.submit(arffSaveCall);
			++userIx;
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

		allAttributes = new FastVector();
		valueDomainMap = Collections
				.synchronizedMap(new HashMap<String, HashMap<String, Attribute>>());

		for (Object statKeyShort : shortColNameDict.keySet()) {
			String statKey = (String) shortColNameDict.get(statKeyShort);
			HashMap<String, Attribute> valueDomain = new HashMap<String, Attribute>();
			valueDomainMap.put(statKey, valueDomain);
			if (Discretize.enumsMap.containsKey(statKey)) {

				for (Enum<?> enumVal : Discretize.enumsMap.get(statKey)) {
					Attribute attribute = new Attribute(statKeyShort.toString()
							+ enumVal.toString());
					valueDomain.put(enumVal.toString(), attribute);
					allAttributes.addElement(attribute);
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
		prevLabelAttribute = new Attribute("prevLabel", labelsVector);
		labelAttribute = new Attribute("label", labelsVector);

		allAttributes.addElement(prevLabelAttribute);
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
				HashMap<Attribute, Integer> countMap = new HashMap<Attribute, Integer>();
				for (int i = 0; i < allAttributes.size(); ++i) {
					countMap.put((Attribute) allAttributes.elementAt(i), 0);
				}

				Reader microLocR = Channels.newReader(FileUtils
						.openInputStream(microLocF).getChannel(),
						Config.OUT_CHARSET);
				int chInt;
				int numTabs = 0;
				StringBuffer header = new StringBuffer();
				
				long currStartTime = Long.parseLong(StringUtils.removeLastNChars(microLocF.getParentFile().getName(),1));
				long currEndTime = Long.parseLong(StringUtils.removeLastNChars(microLocF.getName(),5));
				while ((chInt = microLocR.read()) != -1) {
					if ((char) chInt == '\t') {
						if (numTabs == 0) {
							// inst name.. not useful in weka
						} else if (numTabs == 1) {
							if(prevStartTime != null && prevStartTime.equals(currStartTime)){
								// same visit, different micro loc.. nothing to do
//								System.out.println("dummy!");
							} else if(prevEndTime != null && currStartTime - prevEndTime < Config.INTERVAL_LABEL_CARRY_OVER){
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
								if (!Discretize.enumsMap.containsKey(statKey)) {
									// numeric attribute.. it must be limited to
									// range
									double numVal = Double.parseDouble(value);

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
							} else if (pfx.equals("aid")) {
								HashMap<String, Integer> appFreq = appFreqMap
										.get(microLocF.getName() + microLocF.getParentFile()
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

							countMap.put(attribute, count + 1);

						}
					}
				}

				weka.core.Instance wekaInst = new weka.core.Instance(
						allAttributes.size());
				wekaInst.setDataset(wekaDoc);

				if (instLabel != null) {
					wekaInst.setClassValue(instLabel);
				} else {
					wekaInst.setClassMissing();
				}
				
				if(prevLabel != null){
					wekaInst.setValue(prevLabelAttribute, prevLabel);
				}

				for (Attribute attrib : countMap.keySet()) {
					if (attrib == labelAttribute || attrib == prevLabelAttribute) {
						continue;
					}
					int count = countMap.get(attrib);
					if (count > 0) {
						wekaInst.setValue(attrib, count);
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
