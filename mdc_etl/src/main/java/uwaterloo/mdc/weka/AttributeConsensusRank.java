package uwaterloo.mdc.weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;


public class AttributeConsensusRank implements
		Callable<HashMap<String, HashMap<String, SummaryStatistics>>> {

	// private static final String SELECTED_ATTRS_PFX = "Selected attributes: ";
	// private static final String REDUCED_HEADER_START =
	// "Header of reduced data:";
	private static final String REDUCED_HEADER_END = "@attribute label ";
	private static final String RANKED_ATTRS_PFX = "Ranked attributes:";
	private static final String APPLICATIONS = "APPS";
	public static final String INPUT_PATH = "C:\\mdc-datasets\\weka\\validation_tree-vs-forest\\";
	public static final String OUTPUT_PATH = "C:\\mdc-datasets\\weka\\filters\\";
	private static final String STATNAME_POSTFIX_ATTRS = "_attrs";
	private static final String STATNAME_POSTFIX_CATEGORIES = "_categories";
//	private static final String APPDICT_PROPS_PATH = "C:\\mdc-datasets\\app-uid_name.properties";
	// private static final String STRUCT_DIR_PATH =
	// "/Users/yia/Dropbox/nokia-mdc/";
//	private static final String STRUCT_FILE_PREFIX = "045"; // "struct";
//															// //_q-spread";

	private static final Pattern SPACE_SPLIT = Pattern.compile("\\s+");
	static FileFilter dirFilter = new FileFilter() {
		public boolean accept(File f) {
			return f.isDirectory() && !f.getName().endsWith("PrincipalComponents");
		}
	};
	private static ArrayList<String> allAttributes;

	private final String validationResPath;
	// + "weka.classifiers.trees.J48/";
	// + "weka.classifiers.bayes.NaiveBayesUpdateable/";

	// As explained in http://www.research.ibm.com/cr_aerobatics/cr.html
	private static final Map<String, HashMap<String, Integer>> featFeatSuperiority = Collections
			.synchronizedMap(new HashMap<String, HashMap<String, Integer>>());
	private static final int NUM_ATTRS_TO_RETAIN = 100;
	protected static final String FEAT_SELECTION_FNAME_SUFFIX = "feat-selection.txt";
	private static final String STRUCT_PATH = "C:\\mdc-datasets\\weka\\segmented_user_97\\ALL";
	// private static final Map<String, String> attrIxMap =
	// Collections.synchronizedMap(new HashMap<String, String>());
	private final HashMap<String, HashMap<String, SummaryStatistics>> statsMapArr;
	private static Properties appDictionary;
	private static Map<String, Integer> attrIxes = Collections
			.synchronizedMap(new HashMap<String, Integer>());
	private static Set<Integer> ixSet = Collections
			.synchronizedSet(new HashSet<Integer>());

	public AttributeConsensusRank(String validationResPath) throws IOException {
		statsMapArr = new HashMap<String, HashMap<String, SummaryStatistics>>();

		this.validationResPath = validationResPath;

	}

	public HashMap<String, HashMap<String, SummaryStatistics>> call()
			throws Exception {
		File validationResDir = FileUtils.getFile(validationResPath);

		FilenameFilter fold0AttrSelectionFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("v0")
						&& name.endsWith(FEAT_SELECTION_FNAME_SUFFIX);
			}
		};

		for (File attrSelectionDir : validationResDir.listFiles(dirFilter)) {

			for (File fold0AttrSelectionFile : attrSelectionDir
					.listFiles(fold0AttrSelectionFilter)) {

				final String acrossFoldsSuffix = fold0AttrSelectionFile
						.getName().substring(
								fold0AttrSelectionFile.getName().indexOf('_'));
				String statNameAttrs = attrSelectionDir.getName()
						+ acrossFoldsSuffix + STATNAME_POSTFIX_ATTRS;
				String statNameCategories = attrSelectionDir.getName()
						+ acrossFoldsSuffix + STATNAME_POSTFIX_CATEGORIES;
				for (File attrSelectionFile : attrSelectionDir
						.listFiles(new FilenameFilter() {
							public boolean accept(File dir, String name) {

								return name.endsWith(acrossFoldsSuffix);
							}
						})) {
					HashSet<String> superiorAttrs = new HashSet<String>();

					BufferedReader rd = new BufferedReader(Channels.newReader(
							FileUtils.openInputStream(attrSelectionFile)
									.getChannel(), "US-ASCII"));
					String line;
					boolean inHeader = false;
					int rank = 1;

					while ((line = rd.readLine()) != null) {
						// if(line.startsWith(SELECTED_ATTRS_PFX)){
						// line = line.substring(SELECTED_ATTRS_PFX.length());
						// StringTokenizer attrIxTokens = new
						// StringTokenizer(line,",",false);
						// while(attrIxTokens.hasMoreTokens()){
						//
						// }
						//
						// break;
						// }

						if (!inHeader) {
							inHeader = line.startsWith(RANKED_ATTRS_PFX);
							// inHeader = line.startsWith(REDUCED_HEADER_START);
							// if (inHeader) {
							// // skip the first two lines
							// rd.readLine();
							// rd.readLine();
							// }

						} else {
							inHeader = !line.isEmpty();
							// inHeader = !line.startsWith(REDUCED_HEADER_END);
							if (!inHeader) {
								break;
							}
							String[] lineParts = SPACE_SPLIT.split(line);
							String attrName = lineParts[lineParts.length - 1];
							// String attrIx = lineParts[1];
							// String attrName =
							// line.substring(line.indexOf(' ') +
							// 1,
							// line.lastIndexOf(' '));
							// synchronized(attrIxMap){
							//
							// if (attrName.startsWith("[")) {
							// attrIxMap.put(appDictionary.getProperty(attrName,
							// "SYSTEM"), attrIx);
							// } else {
							// attrIxMap.put(attrName, attrIx);
							// }
							// }

							superiorAttrs.add(attrName);
							synchronized (featFeatSuperiority) {
								HashMap<String, Integer> attrSuperiority = featFeatSuperiority
										.get(attrName);
								for (String otherAttr : attrSuperiority
										.keySet()) {
									if (superiorAttrs.contains(otherAttr)) {
										continue;
									}
									attrSuperiority.put(otherAttr,
											attrSuperiority.get(otherAttr) + 1);
									// This loop could be replaced by the
									// following,
									// but what the hell!
									// + (allAttributes.size() - superiorAttrs
									// .size()));
								}
							}

							if (attrName.startsWith("[")) {
								if (appDictionary.getProperty(attrName) == null) {
									continue;
								}
							}
							addRankToKey(attrName, statNameAttrs, rank);

							// if (attrName.startsWith("[")) {
							// addRankToKey(appDictionary.getProperty(attrName,
							// "SYSTEM"), statNameAttrs, rank);
							// } else {
							// addRankToKey(attrName, statNameAttrs, rank);
							// }

							if (attrName.startsWith("[")) {
								addRankToKey(APPLICATIONS, statNameCategories,
										rank);
							} else if (Character.isDigit(attrName
									.charAt(attrName.length() - 1))) {
								// Numeric value
								int i = 0;
								for (; i < attrName.length(); ++i) {
									if (Character.isDigit(attrName.charAt(i))) {
										break;
									}
								}
								String pfx = attrName.substring(0, i);
								addRankToKey(pfx, statNameCategories, rank);
							} else {
								// Quantized value
								int i = 0;
								for (; i < attrName.length(); ++i) {
									if (Character.isUpperCase(attrName
											.charAt(i))) {
										break;
									}
								}
								String pfx = attrName.substring(0, i);
								addRankToKey(pfx, statNameCategories, rank);
							}
							++rank;
						}
					}
				}

				HashMap<String, SummaryStatistics> statsMap = statsMapArr
						.get(statNameAttrs);
				SortedMap<Double, String> sortedSummaries = new TreeMap<Double, String>();
				for (String attr : statsMap.keySet()) {
					SummaryStatistics stat = statsMap.get(attr);
					Double sortVal = stat.getSum();
					sortedSummaries.put(sortVal, attr);
				}
				printSortedRanking(sortedSummaries, FilenameUtils.concat(validationResDir.getName(), attrSelectionDir.getName()),
						acrossFoldsSuffix);
				printRemoveFilter(sortedSummaries, FilenameUtils.concat(validationResDir.getName(), attrSelectionDir.getName()),
						acrossFoldsSuffix);
			}
		}
		return statsMapArr;
	}

	private void addRankToKey(String attrName, String statName, int rank) {
		HashMap<String, SummaryStatistics> statsMap = statsMapArr.get(statName);
		if (statsMap == null) {
			statsMap = new HashMap<String, SummaryStatistics>();
			statsMapArr.put(statName, statsMap);
		}

		SummaryStatistics attrStat = statsMap.get(attrName);
		if (attrStat == null) {
			attrStat = new SummaryStatistics();
			statsMap.put(attrName, attrStat);
		}
		attrStat.addValue(rank);
	}

	public static void main(String[] args) throws InterruptedException,
			ExecutionException, IOException {
		System.out.println(new Date().toString() + ": Started");

		appDictionary = new Properties();
		appDictionary.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.FEAT_SELECTED_APPS_PATH)));

		allAttributes = new ArrayList<String>();
		int ix = 0;
		File[] structFiles = new File[1];
		structFiles[0] = FileUtils.getFile(STRUCT_PATH, "001.arff");
//		structFiles[0] = FileUtils.getFile(STRUCT_PATH, STRUCT_FILE_PREFIX
//				+ ".app");
		// FileUtils.getFile(BASE_PATH).listFiles(
		// new FilenameFilter() {
		//
		// public boolean accept(File dir, String name) {
		//
		// return name.startsWith(STRUCT_FILE_PREFIX);
		// }
		// });
		for (File structFile : structFiles) {
			BufferedReader rd = new BufferedReader(Channels.newReader(FileUtils
					.openInputStream(structFile).getChannel(), "US-ASCII"));
			// skip the first two lines
			rd.readLine();
			rd.readLine();

			String line;
			while ((line = rd.readLine()) != null) {
				if (line.startsWith(REDUCED_HEADER_END) || line.isEmpty()) {
					break;
				}
				String attrName = line.substring(line.indexOf(' ') + 1,
						line.lastIndexOf(' '));
				// if (attrName.startsWith("[")) {
				// attrName = appDictionary.getProperty(attrName);
				// }
				allAttributes.add(attrName);
				attrIxes.put(attrName, ix);
				ixSet.add(ix);
				++ix;
			}
		}

		ExecutorService countExec = Executors.newSingleThreadExecutor();
		for (File classifierDir : FileUtils.getFile(INPUT_PATH).listFiles(
				dirFilter)) {

			// This is just a dirty workaround to changing the return value
			for (String attrI : allAttributes) {
				HashMap<String, Integer> featSuper = new HashMap<String, Integer>();
				featFeatSuperiority.put(attrI, featSuper);
				for (String attrJ : allAttributes) {
					featSuper.put(attrJ, 0);
				}
			}

			HashMap<String, HashMap<String, SummaryStatistics>> statsMapArr = countExec
					.submit(new AttributeConsensusRank(classifierDir
							.getAbsolutePath())).get();

			HashMap<String, Double> attrsSigmaNXiOverVarMap = new HashMap<String, Double>();
			HashMap<String, Double> attrsSigmaNOverVarMap = new HashMap<String, Double>();
			HashMap<String, Double> catsSigmaNXiOverVarMap = new HashMap<String, Double>();
			HashMap<String, Double> catsSigmaNOverVarMap = new HashMap<String, Double>();

			for (String statName : statsMapArr.keySet()) {
				HashMap<String, SummaryStatistics> statsMap = statsMapArr
						.get(statName);

				String suffix = statName.substring(statName.lastIndexOf("_"));

				HashMap<String, Double> sigmaNXiOverVarMap;
				HashMap<String, Double> sigmaNOverVarMap;
				if (STATNAME_POSTFIX_ATTRS.equals(suffix)) {
					sigmaNXiOverVarMap = attrsSigmaNXiOverVarMap;
					sigmaNOverVarMap = attrsSigmaNOverVarMap;
				} else {
					sigmaNXiOverVarMap = catsSigmaNXiOverVarMap;
					sigmaNOverVarMap = catsSigmaNOverVarMap;
				}

				// // SummaryStatistics nSummary = new SummaryStatistics();
				// int nTotal = 0;
				// for (String attr : statsMap.keySet()) {
				// SummaryStatistics stat = statsMap.get(attr);
				// nTotal +=stat.getN();
				// // nSummary.addValue(stat.getN());
				// }
				// // double nMean = nSummary.getMean();

				SortedMap<Double, String> sortedSummaries = new TreeMap<Double, String>();
				for (String attr : statsMap.keySet()) {
					SummaryStatistics stat = statsMap.get(attr);
					Double sortVal = stat.getSum();
					sortedSummaries.put(sortVal, attr);
				}

				if (STATNAME_POSTFIX_ATTRS.equals(suffix)) {
					printRemoveFilter(sortedSummaries, classifierDir.getName(), "_ALL.txt");
				}
				
				String header = "\"attribute\"\t\"ranking-on\"\t\"n\"\t\"min\"\t\"max\"\t\"mean\"\t\"standard_deviation\"\t\"variance\"\t\"geometric_mean\"\t\"second_moment\"";
				Writer summaryWriter = Channels.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(OUTPUT_PATH + "rank-summary_"
										+ statName + ".csv")).getChannel(),
						"US-ASCII");
				try {
					summaryWriter.append(header).append('\n');
					for (Double sortVal : sortedSummaries.keySet()) {
						String attr = sortedSummaries.get(sortVal);
						SummaryStatistics stat = statsMap.get(attr);
						summaryWriter
								.append(attr.startsWith("[") ? appDictionary
										.getProperty(attr) : attr).append('\t')
								.append("" + sortVal).append('\t')
								.append("" + stat.getN()).append('\t')
								.append("" + stat.getMin()).append('\t')
								.append("" + stat.getMax()).append('\t')
								.append("" + stat.getMean()).append('\t')
								.append("" + stat.getStandardDeviation())
								.append('\t').append("" + stat.getVariance())
								.append('\t')
								.append("" + stat.getGeometricMean())
								.append('\t')
								.append("" + stat.getSecondMoment())
								.append('\n');

						Double sigmaNXiOverVar = sigmaNXiOverVarMap.get(attr);
						if (sigmaNXiOverVar == null) {
							sigmaNXiOverVar = 0.0;
						}
						sigmaNXiOverVar += stat.getN() * stat.getMean();
						if (stat.getVariance() > 0) {
							sigmaNXiOverVar /= stat.getVariance();
						}
						sigmaNXiOverVarMap.put(attr, sigmaNXiOverVar);

						Double sigmaNOverVar = sigmaNOverVarMap.get(attr);
						if (sigmaNOverVar == null) {
							sigmaNOverVar = 0.0;
						}
						sigmaNOverVar += stat.getN();
						if (stat.getVariance() > 0) {
							sigmaNOverVar /= stat.getVariance();
						}
						sigmaNOverVarMap.put(attr, sigmaNOverVar);
					}
				} finally {
					summaryWriter.flush();
					summaryWriter.close();
				}
			}

			SortedMap<Double, String> attrsConsensusMeans = calcAndPrintConsensusMeans(
					attrsSigmaNXiOverVarMap, attrsSigmaNOverVarMap, "overall",
					classifierDir, false);

			calcAndPrintConsensusMeans(catsSigmaNXiOverVarMap,
					catsSigmaNOverVarMap, "overall", classifierDir, false);

			// SortedMap<Double, String> catsConsensusMeans = new
			// TreeMap<Double, String>();
			// for(String cat: catsSigmaNOverVarMap.keySet()){
			// Double catsSigmaNXiOverVar = catsSigmaNXiOverVarMap.get(cat);
			// Double catsSigmaNOverVar = catsSigmaNOverVarMap.get(cat);
			// catsConsensusMeans.put(catsSigmaNXiOverVar / catsSigmaNOverVar,
			// cat);
			// }
			// FileUtils.writeStringToFile(FileUtils.getFile(BASE_PATH,
			// "rank-summary_mean_cats.csv"), catsConsensusMeans.toString());

			SortedMap<Integer, String> featFeatSuperioritySorted = new TreeMap<Integer, String>(
					new Comparator<Integer>() {
						public int compare(Integer o1, Integer o2) {
							return o2.compareTo(o1); // descending
						}
					});
			HashMap<String, Integer> catSuperiority = new HashMap<String, Integer>();
			HashMap<String, Set<String>> catValues = new HashMap<String, Set<String>>();
			SortedMap<Integer, String> catSuperioritySorted = new TreeMap<Integer, String>(
					new Comparator<Integer>() {
						public int compare(Integer o1, Integer o2) {
							return o2.compareTo(o1); // descending
						}
					});

			for (String attrI : featFeatSuperiority.keySet()) {
				HashMap<String, Integer> featSuperiority = featFeatSuperiority
						.get(attrI);
				int sumSuperiority = 0;
				for (String attrJ : featSuperiority.keySet()) {
					sumSuperiority += featSuperiority.get(attrJ);
				}
				String cat;
				if (attrI.startsWith("[")) {
					// attrI = appDictionary.getProperty(attrI);
					cat = APPLICATIONS;
				} else if (Character.isDigit(attrI.charAt(attrI.length() - 1))) {
					// Numeric value
					int i = 0;
					for (; i < attrI.length(); ++i) {
						if (Character.isDigit(attrI.charAt(i))) {
							break;
						}
					}
					cat = attrI.substring(0, i);
				} else {
					// Quantized value
					int i = 0;
					for (; i < attrI.length(); ++i) {
						if (Character.isUpperCase(attrI.charAt(i))) {
							break;
						}
					}
					cat = attrI.substring(0, i);
				}

				Set<String> catSet = catValues.get(cat);
				if (catSet == null) {
					catSet = new HashSet<String>();
					catValues.put(cat, catSet);
				}
				catSet.add(attrI);

				featFeatSuperioritySorted.put(sumSuperiority, attrI);

				Integer catSuper = catSuperiority.get(cat);
				if (catSuper == null) {
					catSuper = 0;
				}
				catSuperiority.put(cat, catSuper + sumSuperiority);
			}

			for (String cat : catSuperiority.keySet()) {
				catSuperioritySorted.put(catSuperiority.get(cat)
						/ catValues.get(cat).size(), cat);
			}

			printSuperiorty(featFeatSuperioritySorted,
					"rank-superiority_attrs.csv", classifierDir);
			printSuperiorty(catSuperioritySorted, "rank-superiority_cats.csv",
					classifierDir);
		}
		countExec.shutdown();
		System.out.println(new Date().toString() + ": Ended");
	}

	private static void printSuperiorty(
			Map<Integer, String> featFeatSuperioritySorted, String filename,
			File classifierDir) throws IOException {

		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(OUTPUT_PATH, classifierDir.getName(), filename))
						.getChannel(), "US-ASCII");
		wr.append("Attr.\tSuperiority\n");
		try {
			for (Integer superiority : featFeatSuperioritySorted.keySet()) {
				String attr = featFeatSuperioritySorted.get(superiority);

				wr.append(
						attr.startsWith("[") ? appDictionary.getProperty(attr)
								: attr).append('\t')
						.append(superiority.toString()).append('\n');
			}
		} finally {
			wr.flush();
			wr.close();
		}
	}

	/**
	 * The Graybill-Deal method as explained in
	 * http://www.itl.nist.gov/div898/software
	 * /dataplot/refman1/auxillar/consmean.htm
	 * 
	 * @param attrsSigmaNXiOverVarMap
	 * @param attrsSigmaNOverVarMap
	 * @param filename
	 * @param classifierDir
	 * @param b
	 * @throws IOException
	 */
	private static SortedMap<Double, String> calcAndPrintConsensusMeans(
			HashMap<String, Double> attrsSigmaNXiOverVarMap,
			HashMap<String, Double> attrsSigmaNOverVarMap, String filename,
			File classifierDir, boolean printRemoveFilter) throws IOException {

		SortedMap<Double, String> attrsConsensusMeans = new TreeMap<Double, String>();
		for (String attr : attrsSigmaNOverVarMap.keySet()) {
			Double attrsSigmaNXiOverVar = attrsSigmaNXiOverVarMap.get(attr);
			Double attrsSigmaNOverVar = attrsSigmaNOverVarMap.get(attr);
			attrsConsensusMeans.put(attrsSigmaNXiOverVar / attrsSigmaNOverVar,
					attr);
		}
		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(OUTPUT_PATH, classifierDir.getName(), filename
								+ "_consensus.csv")).getChannel(), "US-ASCII");
		wr.append("Attr.\tMean\n");
		try {
			for (Double mean : attrsConsensusMeans.keySet()) {
				String attr = attrsConsensusMeans.get(mean);

				wr.append(
						attr.startsWith("[") ? appDictionary.getProperty(attr)
								: attr).append('\t').append(mean.toString())
						.append('\n');
			}

		} finally {
			wr.flush();
			wr.close();
		}

		if (printRemoveFilter) {
			// FIXME printRemoveFilter(attrsConsensusMeans, classifierDir,
			// filename);
			throw new UnsupportedOperationException();
		}

		return attrsConsensusMeans;
	}

	private static void printSortedRanking(
			SortedMap<Double, String> attrsRanking, String dirName,
			String filenameSuffix) throws IOException {
		String header = "\"attribute\"\t\"ranking\"";
		Writer wr = Channels.newWriter(FileUtils.openOutputStream(FileUtils
				.getFile(OUTPUT_PATH, dirName, "rank-summary" + filenameSuffix)).getChannel(),
				"US-ASCII");
		try {
			wr.append(header).append('\n');
			for (Double sortVal : attrsRanking.keySet()) {
				String attr = attrsRanking.get(sortVal);
				wr.append(
						attr.startsWith("[") ? appDictionary.getProperty(attr)
								: attr).append('\t').append("" + sortVal)
						.append('\n');

			}
		} finally {
			wr.flush();
			wr.close();
		}
	}

	private static void printRemoveFilter(
			SortedMap<Double, String> attrsConsensusMeans,
			String dirName, String filenamePfx) throws IOException {
		Object[] sortedAttrNames = attrsConsensusMeans.values().toArray();
		// HashSet<Integer> ixSet = new HashSet<Integer>();
		// for(int i=0; i<allAttributes.size();++i){
		// ixSet.add(i);
		// }
		HashSet<Integer> ixSetCopy = new HashSet<Integer>();
		ixSetCopy.addAll(ixSet);
		for (int a = 0; a < NUM_ATTRS_TO_RETAIN; ++a) {
			Integer attrIx = attrIxes.get(sortedAttrNames[a]);
			if(attrIx == null){
				System.err.println("An attribute that is not part of the structure: " + sortedAttrNames[a] + " ("+ a + ")");
				continue;
			}
			ixSetCopy.remove(attrIx);
		}
		StringBuilder filter = new StringBuilder();
		for (Integer rem : ixSetCopy) {
			filter.append(rem).append(",");
		}
		filter.setLength(filter.length() - 1);
		FileUtils.writeStringToFile(
				FileUtils.getFile(OUTPUT_PATH, dirName, "filter" + filenamePfx),
				filter.toString());
	}

}
