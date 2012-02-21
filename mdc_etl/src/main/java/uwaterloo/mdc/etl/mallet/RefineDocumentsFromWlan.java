package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.Discretize.DurationEunm;
import uwaterloo.mdc.etl.Discretize.RelTimeNWeatherElts;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;
import uwaterloo.mdc.etl.util.TimeFilenameFilter;

public class RefineDocumentsFromWlan
		extends
		CallableOperation<KeyValuePair<String, HashMap<String, Object>>, String> {
	protected static Writer LOG;

	// protected static final String WLAN_NOVISITS_FILENAME =
	// "wlan_no-visit.csv";

	protected static final String COLNAME_AVG_NUM_APS = " avgAps";
	protected static final String COLNAME_STDDEV_NUM_APS = " sdvAps";

	// protected static final String READINGS_AT_SAME_TIME =
	// "TIME_CARDINALITY_";
	protected static final String COLNAME_HOUR_OF_DAY = " hod";
	protected static final String COLNAME_DAY_OF_WEEK = " dow";

	protected static final String COLNAME_TEMPRATURE = " tmp";
	protected static final String COLNAME_SKY = " sky";

	protected String prevTimeZone;

	private final Frequency[] relTimeWStats;
	protected final Frequency visitNoVisitFreq = new Frequency();
	protected final SummaryStatistics durationStats = new SummaryStatistics();
	protected final Frequency durationFreqs = new Frequency();

	protected long prevTime = -1;
	protected long prevprevtime;
	protected long currTime;
	// It seems unuseful: protected long recordDeltaT;
	protected HashMap<String, Integer> prevAccessPoints = null;
	protected HashMap<String, Integer> currAccessPoints;
	protected String currMacAddr = null;

	// This is temporary, not part of the result
	protected SummaryStatistics apsStat = new SummaryStatistics();

	protected HashMap<String, LinkedList<KeyValuePair<String, String>>> userHierarchy = new HashMap<String, LinkedList<KeyValuePair<String, String>>>();
	protected LinkedList<KeyValuePair<String, String>> noVisitHierarchy = new LinkedList<KeyValuePair<String, String>>();

	protected Pattern tabSplit = Pattern.compile("\\t");
	protected long pendingEndTims = -1;
	protected File pendingVisitDir;
	protected SummaryStatistics pendingStats;

	@SuppressWarnings("deprecation")
	public RefineDocumentsFromWlan(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		relTimeWStats = new Frequency[RelTimeNWeatherElts.values().length];
		for (RelTimeNWeatherElts ix : RelTimeNWeatherElts.values()) {
			relTimeWStats[ix.ordinal()] = new Frequency();
		}
	}

	@Override
	protected void eoFileProcedure() throws Exception {
		// Process the last set of observations
		if (pendingEndTims != -1) {
			forceStatsWrite();
		}
	}

	@Override
	protected void headerDelimiterProcedure() throws Exception {
		// Auto-generated method stub

	}

	@Override
	protected void delimiterProcedure() throws Exception {
		if ("time".equals(currKey)) {
			// We'd better let the errors propagate: try {
			currTime = Long.parseLong(currValue);
			if (currTime != prevTime) {
				// A new set of AP sightings (record)
				if (prevTime != -1 && prevAccessPoints != null) {
					// This is not the first sighting in the file:
					// // 1) Update the time delta
					// recordDeltaT = currTime - prevTime;

					// 2) Act upon the difference between it and the prev
					refineDocument();
				}

				// Make the current ones be the prev.s
				prevprevtime = prevTime;
				prevTime = currTime;
				prevAccessPoints = currAccessPoints;
				currAccessPoints = new HashMap<String, Integer>();

			}
			// } catch (NumberFormatException ignored) {
			// // ok!
			// }
		} else if ("tz".equals(currKey)) {
			prevTimeZone = currValue;
		} else if ("mac_address".equals(currKey)) {
			currMacAddr = currValue;

		} else if ("rx".equals(currKey)) {
			currAccessPoints.put(currMacAddr, new Integer(currValue));

		}

	}

	protected void refineDocument() throws IOException {
		long delta;
		double displacementRelAp = 0;
		// long macAddressesDistance = 0;
		for (String mac : currAccessPoints.keySet()) {
			int currRSSI = currAccessPoints.get(mac);
			Integer prevRSSI = prevAccessPoints.remove(mac);
			if (prevRSSI == null) {
				prevRSSI = Config.WLAN_RSSI_MAX;
			}
			displacementRelAp += Discretize.estimateApDistanceLaMarca(Math
					.abs(prevRSSI - currRSSI));

			// macAddressesDistance = macAddressesDistance + ((prevRSSI-currRSSI
			// ) ^ 2);
			// macAddressesDistance = macAddressesDistance +
			// Math.abs(prevRSSI-currRSSI);
		}
		for (Integer prevRssi : prevAccessPoints.values()) {
			// This AP is not visible any more, so add its RSSI
			displacementRelAp += Discretize.estimateApDistanceLaMarca(Math
					.abs((Config.WLAN_RSSI_MAX - prevRssi)));

			// macAddressesDistance = macAddressesDistance +
			// ((Config.WLAN_RSSI_MAX - prevRssi) ^ 2);
			// macAddressesDistance = macAddressesDistance +
			// Math.abs(Config.WLAN_RSSI_MAX - prevRssi);
		}

		// macAddressesDistance = Math.round(Math.log10(macAddressesDistance));
		// macAddressesDistance = Math.round(Math.sqrt(macAddressesDistance));

		File docStartDir = null;
		KeyValuePair<String, String> docEndFile = null;
		LinkedList<KeyValuePair<String, String>> visitHierarchy = null;
		int newDocIx = -1;

		// The macAddrDistance is the average of the displacement according to
		// each AP
		long macAddressesDistance = Math.round(displacementRelAp
				/ (currAccessPoints.size() + prevAccessPoints.size()));
		// TODO: Consider using a Sigmoid function instead of simple
		// TODO: do we need to do anything with recordDelta?
		if (macAddressesDistance >= Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {
			FilenameFilter timeFilter = new TimeFilenameFilter(prevTime);

			File userDir = FileUtils.getFile(outPath, userid);
			for (File visitDir : userDir.listFiles(timeFilter)) {
				int visitStartTime = Integer.parseInt(StringUtils
						.removeLastNChars(visitDir.getName(), 1));
				if (visitStartTime > prevTime) {
					break;
				} else {
					docStartDir = visitDir;
				}
			}

			if (docStartDir != null) {
				visitHierarchy = userHierarchy.get(docStartDir.getName());
				if (visitHierarchy == null) {
					visitHierarchy = new LinkedList<KeyValuePair<String, String>>();
					userHierarchy.put(docStartDir.getName(), visitHierarchy);

					File[] microLocFiles = docStartDir.listFiles();
					Arrays.sort(microLocFiles);
					for (int i = microLocFiles.length - 1; i >= 0; --i) {
						// Descending traversal
						delta = System.currentTimeMillis();
						String placeid = FileUtils
								.readFileToString(microLocFiles[i]);
						delta = System.currentTimeMillis() - delta;
						PerfMon.increment(TimeMetrics.IO_READ, delta);

						visitHierarchy.add(new KeyValuePair<String, String>(
								microLocFiles[i].getName(), placeid));
					}
				}

				newDocIx = 0;
				for (KeyValuePair<String, String> visit : visitHierarchy) {
					int locEndTime = Integer.parseInt(StringUtils
							.removeLastNChars(visit.getKey(), 5));
					if (locEndTime < prevTime) {
						break;
					} else {
						docEndFile = visit;
						// The new doc will be for a time slot that is before
						// the end of the visit, so it should be
						// placed after in the descending list
						++newDocIx;
					}
				}
			}

			if (docEndFile == null) {
				// The end time of the visit was before the record time
				visitNoVisitFreq.addValue(Discretize.VisitReadingBothEnum.R);

				// We are now tracking a new microlocation
				forceStatsWrite();
				return;
			} // else {
			visitNoVisitFreq.addValue(Discretize.VisitReadingBothEnum.B);

			String visitEndTimeStr = StringUtils.removeLastNChars(
					docEndFile.getKey(), 5);

			long visitEndTime = Long.parseLong(visitEndTimeStr);
			if (pendingEndTims != -1 && pendingEndTims != visitEndTime) {
				// log("\tWARNING\tShould have appended the stat for visit ending: "
				// + pendingEndTims);
				forceStatsWrite();
			}

			// Do not split if the reading is towards the end of the
			// visit, because this will result in a fragmented
			// document whose WLAN readings are not specified
			// We also provision for this happening when curr is
			// processed, thus the check for end-curr
			// Sooner or later, in both cases, force will be used
			if ((visitEndTime - prevTime >= Config.WLAN_DELTAT_MIN)
					&& (visitEndTime - currTime >= Config.WLAN_DELTAT_MIN)) {

				long microlocStartTime = prevprevtime;
				if (microlocStartTime == -1) {
					String visitStartDirName = docStartDir.getName();
					microlocStartTime = Long.parseLong(StringUtils
							.removeLastNChars(visitStartDirName, 1));
				}

				String placeId = docEndFile.getValue();
				int tabIx = placeId.indexOf('\t');
				if (tabIx != -1) {
					// This is not the first time the visit is split
					placeId = placeId.substring(0, tabIx);
					if (placeId.indexOf('\t') != -1) {
						log("\tINFO\tOverriding stats for visit: "
								+ docStartDir.getAbsolutePath()
								+ File.separator + docEndFile.getKey());
					}
				}

				Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(prevTime,
						prevTimeZone);

				StringBuilder doc1 = new StringBuilder();
				doc1.append(placeId)
						.append('\t')
						.append(microlocStartTime)
						.append('\t')
						.append(prevTime)
						.append('\t')
						.append(Long.toString(Math.round(apsStat.getMean())))
						.append('\t')
						.append(Long.toString(Math.round(apsStat
								.getStandardDeviation())))
						.append('\t')
						.append(relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
								.ordinal()])
						.append('\t')
						.append(relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
								.ordinal()])
						.append('\t')
						.append(relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
								.ordinal()])
						.append('\t')
						.append(relTimeAndWeather[RelTimeNWeatherElts.SKY
								.ordinal()]);

				KeyValuePair<String, String> newDoc = new KeyValuePair<String, String>(
						Long.toString(prevTime) + Config.TIMETRUSTED_WLAN
								+ ".csv", doc1.toString());
				visitHierarchy.add(newDocIx, newDoc);

				StringBuilder doc2 = new StringBuilder();
				doc2.append(placeId).append('\t').append(currTime).append('\t')
						.append(visitEndTimeStr);

				docEndFile.setValue(doc2.toString());

				// We are now tracking a new microlocation
				apsStat = new SummaryStatistics();
				pendingEndTims = -1;
			} else {
				// We have pending statistics that are not written to the
				// microlocation document. Keep track of them, and next
				// iteration force will be called if needed.
				pendingEndTims = visitEndTime;
				pendingVisitDir = docStartDir;
				pendingStats = apsStat;
			}

		}

		apsStat.addValue(currAccessPoints.size());

	}

	protected void forceStatsWrite() throws IOException {
		if (pendingEndTims == -1) {
			// in case this call is extra
			return;
		}
		if (pendingStats == null
				|| (/* pendingStats == apsStat && */pendingStats.getN() == 0)) {
			// log("\tDEBUG\tCalling force for a stat with no datapoints, supposedly pending: "
			// + visitHierarchy.get(0));
			pendingEndTims = -1;
			return;
		}
		LinkedList<KeyValuePair<String, String>> visitHierarchy = userHierarchy
				.get(pendingVisitDir.getName());
		File docStartDir = pendingVisitDir;

		// Force happens when we have to put the current stats in the last
		// time slot
		// last time slot is the first item in the descending list
		KeyValuePair<String, String> lastTimeSlot = visitHierarchy.get(0);

		// stats
		String mean = Long.toString(Math.round(pendingStats.getMean()));
		String stder = Long.toString(Math.round(pendingStats
				.getStandardDeviation()));

		StringBuilder doc = new StringBuilder();
		String[] docFields = tabSplit.split(lastTimeSlot.getValue());
		if (docFields.length == 1) {
			// only the place id.. visit not split at all
			doc.append(docFields[0])
					.append('\t')
					.append(StringUtils.removeLastNChars(docStartDir.getName(),
							1))
					.append('\t')
					.append(StringUtils.removeLastNChars(lastTimeSlot.getKey(),
							5)).append('\t').append(mean).append('\t')
					.append(stder);
		} else if (docFields.length == 3) {
			// has already updated the time.. leave time itact
			doc.append(docFields[0]).append('\t').append(docFields[1])
					.append('\t').append(docFields[2]).append('\t')
					.append(mean).append('\t').append(stder);
		} else if (docFields.length > 3) {
			// FIXME: I can't find out why this happens.. I'll just leave it :'(
			// LOG that... it shouldn't happen
			log("\tINFO\tForced to override stats for visit: "
					+ docStartDir.getAbsolutePath() + File.separator
					+ lastTimeSlot.getKey());
			if (apsStat != pendingStats) {
				double avg = apsStat.getMean() * apsStat.getN()
						+ pendingStats.getMean() * pendingStats.getN();
				avg /= (apsStat.getN() + pendingStats.getN());

				mean = Long.toString(Math.round(avg));

				// Better than having no value at all.. we use the avg of the
				// two!
				// stder = Config.MISSING_VALUE_PLACEHOLDER;
				double var = (pendingStats.getStandardDeviation() + apsStat
						.getStandardDeviation()) / 2;
				stder = Long.toString(Math.round(var));
			}
			doc.append(docFields[0]).append('\t').append(docFields[1])
					.append('\t').append(docFields[2]).append('\t')
					.append(mean).append('\t').append(stder);
		} else {
			log("\tERROR\tRemoving a file with " + docFields.length
					+ " columns: " + docStartDir.getAbsolutePath()
					+ File.separator + lastTimeSlot.getKey());
			visitHierarchy.remove(0);
			return;
		}

		Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(prevTime,
				prevTimeZone);

		doc.append('\t')
				.append(relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
						.ordinal()])
				.append('\t')
				.append(relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
						.ordinal()])
				.append('\t')
				.append(relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
						.ordinal()]).append('\t')
				.append(relTimeAndWeather[RelTimeNWeatherElts.SKY.ordinal()]);

		lastTimeSlot.setValue(doc.toString());

		// For the next visit
		apsStat = new SummaryStatistics();
		pendingEndTims = -1;
	}

	@Override
	protected void headerDelimiterProcedurePrep() throws Exception {
		// Auto-generated method stub

	}

	@Override
	protected void delimiterProcedurePrep() throws Exception {
		// Auto-generated method stub

	}

	@Override
	protected void writeResults() throws Exception {
		// And then do a check on all the files
		File malletDir = FileUtils.getFile(outPath, /* "mallet", */userid);
		String malletInstFormat = userid + Config.DELIMITER_USER_FEATURE + "%s"
				+ Config.DELIMITER_START_ENDTIME + "%s\t%s\t"
				+ COLNAME_AVG_NUM_APS + Config.DELIMITER_COLNAME_VALUE + "%s"
				+ COLNAME_STDDEV_NUM_APS + Config.DELIMITER_COLNAME_VALUE
				+ "%s" + COLNAME_DAY_OF_WEEK + Config.DELIMITER_COLNAME_VALUE
				+ "%s" + COLNAME_HOUR_OF_DAY + Config.DELIMITER_COLNAME_VALUE
				+ "%s" + COLNAME_TEMPRATURE + Config.DELIMITER_COLNAME_VALUE
				+ "%s" + COLNAME_SKY + Config.DELIMITER_COLNAME_VALUE + "%s";

		File userDir = FileUtils.getFile(outPath, userid);
		long delta;

		for (File visitDir : userDir.listFiles()) {
			if (!visitDir.isDirectory()) {
				continue;
			}

			String malletInst;
			long startTime;
			long endTime;

			LinkedList<KeyValuePair<String, String>> microLocList = userHierarchy
					.get(visitDir.getName());
			if (microLocList == null) {
				File[] visitFiles = visitDir.listFiles();
				assert visitFiles.length == 1 : "Processing a directory with stale files";
				File microLocFile = visitFiles[0];
				log("\tWARNING\tVisit with no WLAN records: "
						+ microLocFile.getAbsolutePath());

				delta = System.currentTimeMillis();
				String microLocInst = FileUtils.readFileToString(microLocFile);
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_READ, delta);

				String startTimeStr = StringUtils.removeLastNChars(
						visitDir.getName(), 1);
				String endTimeStr = StringUtils.removeLastNChars(
						microLocFile.getName(), 5);

				// Assume default timezone
				Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(startTimeStr,
						Config.DEFAULT_TIME_ZONE);

				malletInst = String.format(malletInstFormat, startTimeStr,
						endTimeStr, microLocInst,
						Config.MISSING_VALUE_PLACEHOLDER,
						Config.MISSING_VALUE_PLACEHOLDER,
						relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.SKY.ordinal()]);
				visitNoVisitFreq.addValue(Discretize.VisitReadingBothEnum.V);

				startTime = Long.parseLong(startTimeStr);
				endTime = Long.parseLong(endTimeStr);

				writeMallet(startTime, endTime, malletInst, FileUtils.getFile(
						malletDir, visitDir.getName(), microLocFile.getName()));

			} else {
				for (KeyValuePair<String, String> microLocDoc : microLocList) {
					String[] instFields = tabSplit
							.split(microLocDoc.getValue());
					if (instFields.length == 9) {
						malletInst = String.format(malletInstFormat,
								instFields[1], instFields[2], instFields[0],
								instFields[3], instFields[4], instFields[5],
								instFields[6], instFields[7], instFields[8]);
						startTime = Long.parseLong(instFields[1]);
						// Long.parseLong(instFields[1].substring(0,
						// instFields[1].length() - 1));
						endTime = Long.parseLong(instFields[2]);
						// Long.parseLong(instFields[2].substring(0,
						// instFields[2].length() - 1));
					} else if (instFields.length == 3) {

						startTime = Long.parseLong(instFields[1]);
						endTime = Long.parseLong(instFields[2]);
						// Assume default timezone
						Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(
								startTime, Config.DEFAULT_TIME_ZONE);

						malletInst = String
								.format(malletInstFormat,
										instFields[1],
										instFields[2],
										instFields[0],
										Config.MISSING_VALUE_PLACEHOLDER,
										Config.MISSING_VALUE_PLACEHOLDER,
										relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.SKY
												.ordinal()]);

					} else if (instFields.length == 1) {
						// FIXME: This shouldn't happen.. but DUH!!!! it does!
						String startTimeStr = StringUtils.removeLastNChars(
								visitDir.getName(), 1);
						startTime = Long.parseLong(startTimeStr);
						String endTimeStr = StringUtils.removeLastNChars(
								microLocDoc.getKey(), 5);
						endTime = Long.parseLong(endTimeStr);

						// Assume default timezone
						Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(
								startTime, Config.DEFAULT_TIME_ZONE);

						malletInst = String
								.format(malletInstFormat,
										startTimeStr,
										endTime,
										instFields[0],
										Config.MISSING_VALUE_PLACEHOLDER,
										Config.MISSING_VALUE_PLACEHOLDER,
										relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.SKY
												.ordinal()]);

					} else {
						log("\tERROR\tDiscarding file with wrong number of columns: "
								+ visitDir.getAbsolutePath()
								+ File.separator
								+ microLocDoc.getKey());
						continue;
					}

					writeMallet(startTime, endTime, malletInst,
							FileUtils.getFile(malletDir, visitDir.getName(),
									microLocDoc.getKey()));
				}
			}
		}
	}

	protected Enum<?>[] getRelTimeAndWeather(String startTimeStr,
			String timeZoneStr) {
		return getRelTimeAndWeather(Long.parseLong(startTimeStr), timeZoneStr);
	}

	protected Enum<?>[] getRelTimeAndWeather(long startTime, String timeZoneStr) {
		Enum<?>[] result = Discretize.relTimeNWeather(startTime,
				Config.DEFAULT_TIME_ZONE);

		for (RelTimeNWeatherElts ix : RelTimeNWeatherElts.values()) {
			relTimeWStats[ix.ordinal()].addValue(result[ix.ordinal()]);
		}

		return result;
	}

	protected void writeMallet(long startTime, long endTime, String malletInst,
			File malletFile) throws IOException {
		// Calculate duration
		long durationInSec = endTime - startTime;

		// TODONE: do we need to discritize even more or less?
		DurationEunm durDiscrete = Discretize.duration(durationInSec);

		durationStats.addValue(durationInSec);
		durationFreqs.addValue(durDiscrete);

		malletInst += " dur" + Config.DELIMITER_COLNAME_VALUE
				+ durDiscrete.toString();

		long delta = System.currentTimeMillis();

		FileUtils.writeStringToFile(malletFile, malletInst, Config.OUT_CHARSET);
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.IO_WRITE, delta);
	}

	@Override
	protected String getHeaderLine() throws Exception {
		// Nothing, MALLET doesn't use a header
		return "";
	}

	@Override
	protected KeyValuePair<String, HashMap<String, Object>> getReturnValue()
			throws Exception {
		HashMap<String, Object> result = new HashMap<String, Object>();
		result.put(Config.RESULT_KEY_VISIT_WLAN_BOTH_FREQ, visitNoVisitFreq);
		result.put(Config.RESULT_KEY_DURATION_FREQ, durationFreqs);
		result.put(Config.RESULT_KEY_DURATION_SUMMARY, durationStats);
		result.put(Config.RESULT_KEY_DAY_OF_WEEK_FREQ, relTimeWStats[RelTimeNWeatherElts.DAY_OF_WEEK.ordinal()]);
		result.put(Config.RESULT_KEY_HOUR_OF_DAY_FREQ, relTimeWStats[RelTimeNWeatherElts.HOUR_OF_DAY.ordinal()]);
		result.put(Config.RESULT_KEY_TEMPRATURE_FREQ, relTimeWStats[RelTimeNWeatherElts.TEMPRATURE.ordinal()]);
		result.put(Config.RESULT_KEY_SKY_FREQ, relTimeWStats[RelTimeNWeatherElts.SKY.ordinal()]);
		return new KeyValuePair<String, HashMap<String, Object>>(userid, result);
	}

	public static Writer getLOG() {
		return LOG;
	}

	public static void setLOG(Writer lOG) {
		LOG = lOG;
	}

	protected static void log(String msg) throws IOException {
		long delta = System.currentTimeMillis();
		synchronized (LOG) {
			LOG.append(new SimpleDateFormat().format(new Date())).append(msg)
					.append('\n');
		}
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
	}
}
