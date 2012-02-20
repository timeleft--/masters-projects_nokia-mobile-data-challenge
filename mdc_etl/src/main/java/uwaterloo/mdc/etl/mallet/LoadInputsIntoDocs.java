package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.model.UserVisitsDocsHierarchy;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public abstract class LoadInputsIntoDocs
		extends
		CallableOperation<KeyValuePair<String, HashMap<String, Frequency>>, StringBuilder> {

	// protected static final String READINGS_AT_SAME_TIME =
	// "TIME_CARDINALITY_";
	protected static final String HOUR_OF_DAY = "HOUR_OF_DAY_";
	protected static final String DAY_OF_WEEK = "DAY_OF_WEEK_";

	protected TimeZone timeZoneOfRecord;

	protected Long prevTimeColReading = null;;

	protected final UserVisitsDocsHierarchy userHierarchy;

	protected final Frequency readingNoVisitStat;
	protected final HashMap<String, Frequency> statsMap;

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
				userid));
		readingNoVisitStat = new Frequency();
		statsMap = new HashMap<String, Frequency>();

		statsMap.put(prependFileName(Config.RESULT_KEY_READING_NOVISIT_FREQ),
				readingNoVisitStat);
	}

	protected String prependFileName(String orig) {
		return FilenameUtils.removeExtension(dataFile.getName()) + "_" + orig;
	}

	@Override
	protected void headerEolProcedure() throws Exception {
		// Add two exta columns
		colOpResult.put(HOUR_OF_DAY + getTimeColumnName(), new StringBuilder());
		colOpResult.put(DAY_OF_WEEK + getTimeColumnName(), new StringBuilder());
		statsMap.put(prependFileName(HOUR_OF_DAY + getTimeColumnName()),
				new Frequency());
		statsMap.put(prependFileName(DAY_OF_WEEK + getTimeColumnName()),
				new Frequency());

		super.headerEolProcedure();
	}

	protected void headerDelimiterProcedure() {
		// The value now is the key for later
		colOpResult.put(currValue, new StringBuilder());
		statsMap.put(prependFileName(currValue), new Frequency());
	}

	protected void delimiterProcedure() {

		if (currKey.equals(getTimeColumnName())) {
			// calculateDeltaTime

			long deltaTime = Long.parseLong(currValue);

			if (prevTimeColReading != null) {
				deltaTime -= prevTimeColReading;
				if (deltaTime != 0) {
					// We have finished readings for one time slot.. write
					// them
					onTimeChanged();
				}

			} else {
				// meaningless, because it is the first record
				// System.out.println("blah.. just making sure of something!");
			}
			prevTimeColReading = Long.parseLong(currValue);
		} else if ("tz".equals(currKey)) {
			char timeZonePlusMinus = '+';
			if (currValue.charAt(0) == '-') {
				timeZonePlusMinus = '-';
			}
			// Offset in hours (from seconds)
			int timeZoneOffset = 0;
			try {
				timeZoneOffset = Integer.parseInt(currValue.substring(1)) / 3600;
			} catch (NumberFormatException ex) {
				// Ok calm down!
			}
			timeZoneOfRecord = TimeZone.getTimeZone("GMT" + timeZonePlusMinus
					+ timeZoneOffset);
		} else {
			colOpResult.get(currKey).append(" ").append(shortKey())
					.append(Config.DELIMITER_COLNAME_VALUE)
					.append(getValueToWrite());
			statsMap.get(prependFileName(currKey)).addValue(getValueToWrite());
		}
	}

	private String shortKey() {
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

				result += currKey.charAt(0);

				int underscoreIx = 0;
				while ((underscoreIx = currKey.indexOf("_", underscoreIx)) != -1) {
					result += currKey.charAt(underscoreIx);
				}

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
				StringBuilder doc = userHierarchy.getDocDirect(visitStartTime,
						endTimeInSecs);

				if (doc == null) {
					readingNoVisitStat
							.addValue(Discretize.VisitReadingBothEnum.V);
					continue;
				}

				File malletFile = FileUtils.getFile(visitDir, "mallet_"
						+ microLocFile.getName());
				// IMPORTANT.. no synchronization here.. I depend on having
				// one thread per user!
				long delta = System.currentTimeMillis();
				FileUtils.writeStringToFile(malletFile, doc.toString(), true);
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

	private void onTimeChanged() {
		StringBuilder docBuilder = userHierarchy
				.getDocForEndTime(prevTimeColReading);
		if (docBuilder == null) {
			// This reading doesn't have an associated visit
			readingNoVisitStat.addValue(Discretize.VisitReadingBothEnum.R);
			for (StringBuilder colBuilder : colOpResult.values()) {
				// Discard the readings
				colBuilder.setLength(0);
			}

		} else {
			readingNoVisitStat.addValue(Discretize.VisitReadingBothEnum.B);

			StringBuilder strTime = colOpResult.get(getTimeColumnName());
			if (strTime != null && strTime.length() != 0) {
				Long longTime = Long.parseLong(strTime.toString()) * 1000;
				Calendar calendar = Calendar.getInstance(timeZoneOfRecord);
				calendar.setTimeInMillis(longTime);

				int hOfDay = calendar.get(Calendar.HOUR_OF_DAY);
				int dOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

				// TODO: should we write only when the hour or day change?
				colOpResult.get(HOUR_OF_DAY + getTimeColumnName()).append(" ")
						.append(Config.COLNAME_HOUR_OF_DAY)
						.append(Config.DELIMITER_COLNAME_VALUE).append(hOfDay);
				// FIXME: (IMPORTANT) append weather
				colOpResult.get(DAY_OF_WEEK + getTimeColumnName()).append(" ")
						.append(Config.COLNAME_DAY_OF_WEEK)
						.append(Config.DELIMITER_COLNAME_VALUE).append(dOfWeek);

				statsMap.get(prependFileName(HOUR_OF_DAY + getTimeColumnName()))
						.addValue(hOfDay);
				statsMap.get(prependFileName(DAY_OF_WEEK + getTimeColumnName()))
						.addValue(dOfWeek);
			}

			for (StringBuilder colBuilder : colOpResult.values()) {
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
	protected KeyValuePair<String, HashMap<String, Frequency>> getReturnValue()
			throws Exception {

		KeyValuePair<String, HashMap<String, Frequency>> result = new KeyValuePair<String, HashMap<String, Frequency>>(
				userid, statsMap);

		return result;
	}

	protected abstract String getValueToWrite();
}
