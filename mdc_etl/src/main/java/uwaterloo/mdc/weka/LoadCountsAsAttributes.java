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

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.mallet.LoadInputsIntoDocs;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;
import weka.core.Attribute;
import weka.core.FastVector;
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

		}

		@Override
		public Void call() throws Exception {
			File dest = FileUtils.getFile(outPath,
					insts.relationName()+".arff");
			System.out.println((System.currentTimeMillis() - time) + ": Writing " + dest.getAbsolutePath());

			ArffSaver arffsaver = new ArffSaver();
			arffsaver.setInstances(insts);
			arffsaver.setDestination(FileUtils.openOutputStream(dest));
//			arffsaver.setCompressOutput(true);
			arffsaver.writeBatch();
			System.out.println((System.currentTimeMillis() - time) + ": Finished writing " + dest.getAbsolutePath());

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
	private int countingJobs;

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

		// for (int i = 0; i < printingJobs; ++i) {
		// printEcs.take();
		// }
		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		printExec.shutdown();
		// Wait until all threads are finish
		while (!printExec.isTerminated()) {
			Thread.sleep(5000);
			System.out.println((System.currentTimeMillis() - time) + ": Shutting down");
		}
		
		app.countExec.shutdown();
		
		System.out.println(new Date() + ": Done in " + (System.currentTimeMillis() - time) + " millis");
		
		

	}

	public ExecutorCompletionService<Instances> call() throws Exception {

		return this.count();

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
				for (int i = 1; i <= MathUtil.pows2.length; ++i) {
					Attribute attribute = new Attribute(statKeyShort.toString()
							+ (i));
					valueDomain.put(Integer.toString(i), attribute);
					allAttributes.addElement(attribute);
				}

			}
		}
		FastVector labelsVector = new FastVector();
		for (String label : Config.LABELS) {
			labelsVector.addElement(label);
		}
		labelAttribute = new Attribute("label", labelsVector);

		allAttributes.addElement(labelAttribute);

	}

	public ExecutorCompletionService<Instances> count() throws Exception {
		File dataDir = FileUtils.getFile(inputPath);

		for (File userDir : dataDir.listFiles()) {
			CountingCallable countCall = new CountingCallable(userDir);
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

		public Instances call() throws Exception {

			System.out.println((System.currentTimeMillis() - time) + ": Reading user " + userDir.getName());

			Collection<File> microLocsFiles = FileUtils.listFiles(userDir,
					new String[] { "csv" }, true);
			weka.core.Instances wekaDoc = new weka.core.Instances(
					userDir.getName(), allAttributes, microLocsFiles.size());
			wekaDoc.setClassIndex(labelAttribute.index());

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
				String instLabel = null;
				while ((chInt = microLocR.read()) != -1) {
					if ((char) chInt == '\t') {
						if (numTabs == 0) {
							// inst name.. not useful in weka
						} else if (numTabs == 1) {
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

							HashMap<String, Attribute> valueDomain = valueDomainMap
									.get(statKey);
							Attribute attribute;
							if (valueDomain != null) {
								if (!Discretize.enumsMap.containsKey(statKey)
										&& (pfx.startsWith("avg") || pfx
												.startsWith("sdv"))) {
									// numeric value that need smoothing
									double numVal = Double.parseDouble(value);
									if (numVal < MathUtil.pows2[0]) {
//										numVal = MathUtil.pows2[0];
										// negative or zero.. discard!
										continue;
									} else if (numVal > MathUtil.pows2[MathUtil.pows2.length - 1]) {
										numVal = MathUtil.pows2[MathUtil.pows2.length - 1];
									}
									value = Long.toString(MathUtil
											.lgSmoothing(numVal));

								}
								attribute = valueDomain.get(value);

							} else {
								if (!pfx.isEmpty() && !pfx.equals("aid")) {
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

				for (Attribute attrib : countMap.keySet()) {
					if (attrib == labelAttribute) {
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
			System.out.println((System.currentTimeMillis() - time) + ": Finished reading user " + userDir.getName());			
			return wekaDoc;
		}

		protected CountingCallable(File userDir) {
			// targetStats.clear();
			this.userDir = userDir;
		}
	}

}
