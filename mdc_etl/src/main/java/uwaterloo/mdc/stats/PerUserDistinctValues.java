package uwaterloo.mdc.stats;

import java.io.File;
import java.io.Writer;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;

public class PerUserDistinctValues extends
		CallableOperation<SummaryStatistics, Frequency> {
	public static class KeyIntegerValue<K> implements Map.Entry<K, Integer> {
		protected K key;
		protected Integer value;

		public KeyIntegerValue(K key, Integer value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		public Integer getValue() {
			return value;
		}

		public Integer setValue(Integer value) {
			this.value = value;
			return value;
		}

	}

	protected static final String READINGS_AT_SAME_TIME = "TIME_CARDINALITY_";
	protected static final String HOUR_OF_DAY = "HOUR_OF_DAY_";
	protected static final String DAY_OF_WEEK = "DAY_OF_WEEK_";

	protected TimeZone timeZoneOfRecord;

	protected HashMap<String, KeyIntegerValue<String>> prevTimeColsReadings = new HashMap<String, KeyIntegerValue<String>>();

	protected SummaryStatistics timeDifferenceStatistics = new SummaryStatistics();

	@Deprecated
	public PerUserDistinctValues(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}

	@Override
	protected void headerEolProcedure() throws Exception {
		for (String key : keyList) {
			if (key.contains("time")) {
				// toUpperCase prevent catching these fields in the write
				// results function
				String upperCaseKey = key.toUpperCase();
				colOpResult.put(READINGS_AT_SAME_TIME + upperCaseKey,
						new Frequency());
				colOpResult.put(HOUR_OF_DAY + upperCaseKey, new Frequency());
				colOpResult.put(DAY_OF_WEEK + upperCaseKey, new Frequency());
			}
		}
		super.headerEolProcedure();
	}

	protected void headerDelimiterProcedure() {
		colOpResult.put(currValue, new Frequency());
	}

	protected void delimiterProcedure() {
		colOpResult.get(currKey).addValue(currValue);

		if (currKey.equals(getTimeColumnName())) {
			// calculateDeltaTime
			try {
				long deltaTime = Long.parseLong(currValue);

				String upperCaseTimeKey = getTimeColumnName().toUpperCase();
				KeyIntegerValue<String> prevReading = prevTimeColsReadings
						.get(upperCaseTimeKey);
				if (prevReading != null) {
					deltaTime -= Long.parseLong(prevReading.getKey());
					timeDifferenceStatistics.addValue(deltaTime);
				} else {
					// meaningless, because it is probably the first record
					// System.out.println("blah.. just making sure of something!");
				}
			} catch (NumberFormatException ex) {
				// Ok calm down!
			}
		}

		if (currKey.contains("time")) {
			KeyIntegerValue<String> prevReading = prevTimeColsReadings
					.get(currKey);
			if (prevReading == null) {
				// initialization
				prevReading = new KeyIntegerValue<String>(currValue, 0);
				prevTimeColsReadings.put(currKey.toUpperCase(), prevReading);
			}

			if (prevReading.getKey().equals(currValue)) {
				// Another reading at the same time
				prevReading.setValue(prevReading.getValue() + 1);
			} else {
				colOpResult.get(READINGS_AT_SAME_TIME + currKey.toUpperCase())
						.addValue(prevReading.getValue().toString());

				prevReading.setKey(currValue);
				prevReading.setValue(1);
			}
		} else if (currKey.startsWith("tz")) {
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
		}
	}

	@Override
	protected void eolProcedure() throws Exception {
		if (prevTimeColsReadings != null) { // avoid header's eol
			for (String key : colOpResult.keySet()) {
				if (key.contains("time")) {
					String upperCaseKey = key.toUpperCase();
					KeyIntegerValue<String> prevReading = prevTimeColsReadings
							.get(upperCaseKey);
					long longTime = Long.parseLong(prevReading.getKey()) * 1000;
					Calendar calendar = Calendar.getInstance(timeZoneOfRecord);
					calendar.setTimeInMillis(longTime);

					colOpResult.get(HOUR_OF_DAY + upperCaseKey).addValue(
							"" + calendar.get(Calendar.HOUR_OF_DAY));
					colOpResult.get(DAY_OF_WEEK + upperCaseKey).addValue(
							"" + calendar.get(Calendar.DAY_OF_WEEK));
				}
			}
		}

		super.eolProcedure();
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
		long delta;
		for (String key : colOpResult.keySet()) {
			if (Config.USERID_COLNAME.equals(key)) {
				continue;
			}

			String freqFileName = "counts_"
					+ FilenameUtils.removeExtension(dataFile.getName()) + "-"
					+ key + "-" + userid + ".csv";
			Writer freqWriter = acquireWriter(freqFileName,
					((CalcPerUserStats) master).freqWriterMap,
					((CalcPerUserStats) master).freqWriterLocks);
			try {
				Frequency freq = colOpResult.get(key);

				if (key.contains("time")) {
					// Time frequencies are not useful and are very hard to
					// calculate
					// because of the large number of different values
					int uniqueCount = freq.getUniqueCount();
					delta = System.currentTimeMillis();
					freqWriter.append(userid).append('\t').append("ALL")
							.append('\t').append(Integer.toString(uniqueCount))
							.append('\t').append("N/A").append('\n');
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.IO_WRITE, delta);

					continue;
				}

				Iterator<Comparable<?>> vIter = freq.valuesIterator();
				while (vIter.hasNext()) {
					String val = (String) vIter.next();

					long valCount = freq.getCount(val);
					double valPct = freq.getPct(val);

					delta = System.currentTimeMillis();
					freqWriter.append(userid).append('\t').append(val)
							.append('\t').append(Long.toString(valCount))
							.append('\t').append(Double.toString(valPct))
							.append('\n');
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.IO_WRITE, delta);
				}
			} finally {
				releaseWriter(freqWriter, freqFileName,
						((CalcPerUserStats) master).freqWriterMap,
						((CalcPerUserStats) master).freqWriterLocks);
			}
		}

	}

	protected String getHeaderLine() {
		return "userid\tvalue\tcount\tpctage\n";
	}

	@Override
	protected void eoFileProcedure() {
		for (String key : colOpResult.keySet()) {
			if (key.startsWith(READINGS_AT_SAME_TIME)) {
				KeyIntegerValue<String> lastReading = prevTimeColsReadings
						.get(key.substring(READINGS_AT_SAME_TIME.length()));
				if (lastReading == null) {
					// no previous readings
					continue;
				}
				colOpResult.get(key)
						.addValue(lastReading.getValue().toString());
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
	protected SummaryStatistics getReturnValue() {
		return timeDifferenceStatistics;
	}
}