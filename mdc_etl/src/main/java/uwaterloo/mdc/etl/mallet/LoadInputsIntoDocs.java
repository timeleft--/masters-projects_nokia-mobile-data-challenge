package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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

public abstract class LoadInputsIntoDocs
		extends
		CallableOperation<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> {

	private static final String CONTINUOUS_POSTFIX = "C";

	protected Long prevTimeColReading = null;;

	protected final UserVisitsDocsHierarchy userHierarchy;

	protected final Frequency readingNoVisitStat = new Frequency();
	protected final Frequency visitNoReadingStat = new Frequency();
	protected final HashMap<String, Object> statsMap = new HashMap<String, Object>();

	protected long currTime = 0;

	// Trading memory for performance: removed and (incomplete) synchronization
	// But we need a global place for storing all labels, to avoid duplocates
	private static Map<String, String> shortColLabelsMap = Collections
			.synchronizedMap(new HashMap<String, String>());

	// private Map<String, String> shortColLabelsMap = new HashMap<String,
	// String>();

	@SuppressWarnings("deprecation")
	public LoadInputsIntoDocs(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

		userHierarchy = new UserVisitsDocsHierarchy(FileUtils.getFile(outPath,
				dataFile.getParentFile().getName()));

		statsMap.put(prependFileName(Config.RESULT_KEY_READING_NOVISIT_FREQ),
				readingNoVisitStat);
		statsMap.put(prependFileName(Config.RESULT_KEY_VISIT_NOREADING_FREQ),
				visitNoReadingStat);
		// This a special stats.. we's better handle it when getting the values
		// from the map by recognizing this special frequency stats
		// Discretize.enumsMap.add(prependFileName(Config.RESULT_KEY_READING_NOVISIT_FREQ),
		// Discretize.VisitReadingBothEnum.values());

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
	}

	protected void delimiterProcedure() {
		if(getColsToSkip().contains(currKey)){
			return;
		}

		if (currKey.equals(getTimeColumnName())) {
			// calculateDeltaTime

			currTime = Long.parseLong(currValue);
			
		} else if ("tz".equals(currKey)) {
			// We keep times in GMT.. 
			currTime += Long.parseLong(currValue);
			
			if (prevTimeColReading != null) {
				long deltaTime = currTime - prevTimeColReading;
				if (deltaTime != 0) {
					// We have finished readings for one time slot.. write
					// them
					onTimeChanged();
				}

			} else {
				// meaningless, because it is the first record
				// System.out.println("blah.. just making sure of something!");
			}
			prevTimeColReading = currTime;
			currTime = 0;

		} else {
			Comparable<?> discreteVal = getValueToWrite();
			appendCurrValToCol(discreteVal);
			addCurrValToStats(discreteVal);
		}
	}

	protected abstract HashSet<String> getColsToSkip();

	protected void appendCurrValToCol(Comparable<?> discreteVal) {
		// Don't put place holder values (like 0) in the continuous stat
		if(Config.MISSING_VALUE_PLACEHOLDER.equals(discreteVal.toString())) {
			return;
		}
		
		colOpResult.get(currKey).append(" ").append(shortKey())
				.append(Config.DELIMITER_COLNAME_VALUE)
				.append(getValueToWrite());
	}

	protected void addCurrValToStats(Comparable<?> discreteVal) {
		Object statsObj = statsMap.get(prependFileName(currKey));
		if(statsObj == null){
			// In case of values that are not enums
			return;
		}
		((Frequency) statsObj).addValue(discreteVal);

		if (keepContinuousStatsForColumn(currKey) 
				// Don't put place holder values (like 0) in the continuous stat
				&& !Config.MISSING_VALUE_PLACEHOLDER.equals(discreteVal.toString())) {
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
		String result;

		String key = dataFile.getName() + currKey;

		long delta = System.currentTimeMillis();
		synchronized (shortColLabelsMap) {
			result = shortColLabelsMap.get(key);
			if (result == null) {
				if ("accel.csv".equals(dataFile.getName())) {
					result = "ac";
				} else if ("application.csv".equals(dataFile.getName())) {
					result = "ap";
				} else if ("bluetooth.csv".equals(dataFile.getName())) {
					result = "b";
				} else if ("calendar.csv".equals(dataFile.getName())) {
					result = "cr";
				} else if ("calllog.csv".equals(dataFile.getName())) {
					result = "cg";
				} else if ("contacts.csv".equals(dataFile.getName())) {
					result = "cs";
				} else if ("gsm.csv".equals(dataFile.getName())) {
					result = "g";
				} else if ("media.csv".equals(dataFile.getName())) {
					result = "md";
				} else if ("mediaplay.csv".equals(dataFile.getName())) {
					result = "mp";
				} else if ("process.csv".equals(dataFile.getName())) {
					result = "p";
				} else if ("sys.csv".equals(dataFile.getName())) {
					result = "s";
				} else {
					throw new IllegalArgumentException(
							"Check the spelling of the filename in the above list");
				}

				int underscoreIx = -1;
				do {
					result += currKey.charAt(underscoreIx + 1);
					underscoreIx = currKey.indexOf("_", underscoreIx + 1);
				} while (underscoreIx != -1);

				if (shortColLabelsMap.containsValue(result)) {
					int i = 2;
					while (shortColLabelsMap.containsValue(result + i)) {
						++i;
					}
					result += i;
				}

				shortColLabelsMap.put(key, result);
			}

		}
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);

		return result;
	}

	/**
	 * Since the CSVs of MDC represent time series, the column representing time
	 * must be treated specially.
	 * 
	 * @return The anem of the column representing time
	 */
	protected String getTimeColumnName() {
		return "time";
	}

	protected void writeResults() throws Exception {
		if(userid == null){
			// this file is empty
			return;
		}
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

				if (doc == null) {
					visitNoReadingStat.addValue(VisitWithReadingEnum.V);
//					readingNoVisitStat
//							.addValue(Discretize.VisitReadingBothEnum.V);
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
			// The records of the last time
			onTimeChanged();
		} else {
			// meaningless, because it is the first record
			// System.out.println("blah.. just making sure of something!");
		}
	}

	protected final void onTimeChanged() {
		StringBuilder docBuilder = userHierarchy
				.getDocForEndTime(prevTimeColReading);
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
