package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.Discretize.ReadingWithinVisitEnum;
import uwaterloo.mdc.etl.Discretize.VisitWithReadingEnum;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.model.UserVisitsDocsHierarchy;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

/**
 * 
 * @author yaboulna
 * 
 */
public abstract class LoadInputsIntoDocs
		extends
		CallableOperation<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> {
	protected Pattern tabSplit = Pattern.compile("\\t");
	private static final String CONTINUOUS_POSTFIX = "C";

	private String quantizationPath = (Config.WEKA_DISCRETIZE ? "C:\\mdc-datasets\\weka\\cutpoints"
			: "P:\\mdc-datasets\\quantization");

	protected Long prevTimeColReading = null;

	protected final UserVisitsDocsHierarchy<StringBuilder> userHierarchy;

	protected final Frequency readingNoVisitStat = new Frequency();
	protected final Frequency visitNoReadingStat = new Frequency();

	/**
	 * For a frequency to be included in the result, its possible values must be
	 * added to the Discritize.enumsMap with a key that is FILENAME_COLNAME.
	 * This field is the result.
	 */
	protected final HashMap<String, Object> statsMap = new HashMap<String, Object>();

	protected long currTime = 0;

	private StringBuilder prevDocBuilder;

	protected HashMap<String, double[]> colQuantizationMap = new HashMap<String, double[]>();

	@SuppressWarnings("deprecation")
	public LoadInputsIntoDocs(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

		userid = dataFile.getParentFile().getName();
		userHierarchy = new UserVisitsDocsHierarchy<StringBuilder>(
				FileUtils.getFile(outPath, userid),
				StringBuilder.class.getConstructor());

		statsMap.put(prependFileName(Config.RESULT_KEY_READING_NOVISIT_FREQ),
				readingNoVisitStat);
		statsMap.put(prependFileName(Config.RESULT_KEY_VISIT_NOREADING_FREQ),
				visitNoReadingStat);
	}

	protected String prependFileName(String orig) {
		return FilenameUtils.removeExtension(dataFile.getName()) + "_" + orig;
	}

	protected void headerDelimiterProcedure() {
		// The value now is the key for later
		colOpResult.put(currValue, new StringBuilder());

		// The static sections in subclasses will fill the enumMap
		if (Discretize.enumsMap.containsKey(prependFileName(currValue))) {
			statsMap.put(prependFileName(currValue), new Frequency());

			if (keepContinuousStatsForColumn(currValue)) {
				statsMap.put(prependFileName(currValue + CONTINUOUS_POSTFIX),
						new SummaryStatistics());
			}
		}
		if ((Config.QUANTIZE_NOT_DISCRETIZE || Config.WEKA_DISCRETIZE)
				&& !getColsToSkip().contains(currValue)
				// Force it not to quantize the nominal values
				&& !"in_phonebook".equals(currValue)
				&& !"charging".equals(currValue)) {

			double[] quants = new double[Config.NUM_QUANTILES];

			String quantFName = FilenameUtils.removeExtension(dataFile
					.getName()) + "_" + currValue;
			if (Config.WEKA_DISCRETIZE) {
				quantFName += "_quantiles.csv";
			} else {
				quantFName = Config.quantizedFields.getProperty(quantFName);
			}
			if (quantFName != null) {
				if (Config.WEKA_DISCRETIZE) {
					quantFName = "weka_" + quantFName;
				} else {
					if (Config.QUANTIZATION_PER_USER) {
						quantFName = "mean_" + quantFName;
					} else {
						quantFName = userid + "_" + quantFName;
					}
				}
				File quantFile = FileUtils
						.getFile(quantizationPath, quantFName);
				if (quantFile.exists()) {
					try {
						String quantStr = FileUtils.readFileToString(quantFile).trim();
						if (quantStr != null && !quantStr.isEmpty()) {
							String[] boundaryArr = tabSplit.split(quantStr);
							for (int b = 0; b < boundaryArr.length; ++b) {
								String boundary = boundaryArr[b];
								quants[b] = Double.parseDouble(boundary);
							}
							colQuantizationMap.put(currValue, quants);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	protected void delimiterProcedure() {
		if (getColsToSkip().contains(currKey)) {
			return;
		}

		if (currKey.equals(getTimeColumnName())) {
			currTime = Long.parseLong(currValue);

			// } else if ("tz".equals(currKey)) {
			// // We keep times in GMT..
			// currTime += Long.parseLong(currValue);

			if (prevTimeColReading != null) {
				long deltaTime = currTime - prevTimeColReading;
				if (deltaTime != 0) {
					// We have finished readings for one time slot.. write
					onTimeChanged();
				}
			}
			prevTimeColReading = currTime;
			currTime = 0;
		} else if ("tz".equals(currKey)) {
			// just skipping this column
		} else {
			Comparable<?> discreteVal;
			discreteVal = getValueToWrite();

			double[] boundaries = colQuantizationMap.get(currKey);
			if ((Config.QUANTIZE_NOT_DISCRETIZE || Config.WEKA_DISCRETIZE)
					&& boundaries != null
					// override the value only if there is one
					&& discreteVal != null
					&& !discreteVal.toString().equals(
							Config.MISSING_VALUE_PLACEHOLDER)) {

				Double contVal = new Double(currValue);
				if (contVal.isNaN() || contVal.isInfinite()) {
					discreteVal = null;
				} else {
					int d = 0;
					for (; d < boundaries.length; ++d) {
						if (boundaries[d] > contVal) {
							break;
						}
					}
					discreteVal = Discretize.QuantilesEnum.values()[d];
				}
			}

			// Don't put place holder values (like 0) in the continuous stat
			if (discreteVal != null && // comment below when adding new
										// discritizers
										// (to allow null pointer exc)
					!Config.MISSING_VALUE_PLACEHOLDER.equals(discreteVal
							.toString())) {
				appendCurrValToCol(discreteVal);
				addCurrValToStats(discreteVal);
			}
		}
	}

	public abstract HashSet<String> getColsToSkip();

	protected void appendCurrValToCol(Comparable<?> discreteVal) {
		// Moved to caller
		// // Don't put place holder values (like 0) in the continuous stat
		// if (discreteVal == null || // comment below when adding new
		// discritizers
		// // (to allow null pointer exc)
		// Config.MISSING_VALUE_PLACEHOLDER.equals(discreteVal.toString())) {
		// return;
		// }

		colOpResult.get(currKey).append(" ").append(shortKey())
				.append(Config.DELIMITER_COLNAME_VALUE)
				.append(discreteVal.toString());
	}

	protected void addCurrValToStats(Comparable<?> discreteVal) {
		Object statsObj = statsMap.get(prependFileName(currKey));
		if (statsObj == null) {
			// // comment for debugging new discretizers
			// || discreteVal == null) {
			// // In case of values that are not enums
			return;
		}
		((Frequency) statsObj).addValue(discreteVal);

		if (keepContinuousStatsForColumn(currKey)
		// Don't put place holder values (like 0) in the continuous stat
				&& !Config.MISSING_VALUE_PLACEHOLDER.equals(discreteVal
						.toString())) {
			statsObj = statsMap.get(prependFileName(currKey
					+ CONTINUOUS_POSTFIX));
			try {
				((SummaryStatistics) statsObj).addValue(Double
						.parseDouble(currValue));
			} catch (NumberFormatException ignored) {
				// might be a NaN or a ?.. ignore
			}
		}
	}

	protected String shortKey() {
		return Discretize.getShortKey(dataFile, currKey);
	}

	/**
	 * Since the CSVs of MDC represent time series, the column representing time
	 * must be treated specially.
	 * 
	 * @return The anem of the column representing time
	 */
	public String getTimeColumnName() {
		return "time";
	}

	protected void writeResults() throws Exception {
		// if (userid == null) {
		// // this file is empty
		// return;
		// }
		// We do a check on all the files for stats gathering
		File userDir = FileUtils.getFile(outPath, userid);

		for (File visitDir : userDir.listFiles()) {
			if (!visitDir.isDirectory()) {
				continue;
			}

			Long visitStartTime = Long.parseLong(StringUtils.removeLastNChars(
					visitDir.getName(), 1));
			for (File microLocFile : visitDir.listFiles()) {

				Long endTimeInSecs = Long.parseLong(StringUtils
						.removeLastNChars(microLocFile.getName(), 5));
				StringBuilder doc = userHierarchy.getDocExact(visitStartTime,
						endTimeInSecs);

				if (doc == null || doc.length() == 0) {
					visitNoReadingStat.addValue(VisitWithReadingEnum.V);
					continue;
				} // else
				visitNoReadingStat.addValue(VisitWithReadingEnum.B);

				// IMPORTANT.. no synchronization here.. I depend on having
				// one thread per user!
				long delta = System.currentTimeMillis();
				FileUtils.writeStringToFile(microLocFile, doc.toString(), true);
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_WRITE, delta);
			}
		}
	}

	protected String getHeaderLine() {
		// No header for mallet
		return "";
	}

	@Override
	protected void eoFileProcedure() {
		if (prevTimeColReading != null) {
			onMicroLocChange();
			// The records of the last time
			onTimeChanged();
		}
	}

	protected void onTimeChanged() {
		StringBuilder docBuilder = userHierarchy
				.getDocForEndTime(prevTimeColReading);
		if (prevDocBuilder != null && !prevDocBuilder.equals(docBuilder)) {
			onMicroLocChange();
		}
		if (docBuilder == null) {
			// This reading doesn't have an associated visit
			readingNoVisitStat.addValue(ReadingWithinVisitEnum.R);
			for (StringBuilder colBuilder : colOpResult.values()) {
				// Discard the readings
				colBuilder.setLength(0);
			}

		} else {
			readingNoVisitStat.addValue(ReadingWithinVisitEnum.B);

			for (StringBuilder colBuilder : colOpResult.values()) {
				if (colBuilder.length() == 0) {
					continue; // nothing in this column.. don't append spaces!
				}
				docBuilder.append(" ").append(colBuilder.toString());
				colBuilder.setLength(0);

			}
		}
		prevDocBuilder = docBuilder;
	}

	protected void onMicroLocChange() {
	}

	@Override
	protected void headerDelimiterProcedurePrep() {
		// nothin

	}

	@Override
	protected void delimiterProcedurePrep() {
		// nothin

	}

	@Override
	protected KeyValuePair<String, HashMap<String, Object>> getReturnValue()
			throws Exception {

		KeyValuePair<String, HashMap<String, Object>> result = new KeyValuePair<String, HashMap<String, Object>>(
				userid, statsMap);

		return result;
	}

	protected abstract Comparable<?> getValueToWrite();

	protected abstract boolean keepContinuousStatsForColumn(String colName);
}
