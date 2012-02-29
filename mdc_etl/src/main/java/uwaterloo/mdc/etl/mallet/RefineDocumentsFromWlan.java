package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.Discretize.DurationEunm;
import uwaterloo.mdc.etl.Discretize.ReadingWithinVisitEnum;
import uwaterloo.mdc.etl.Discretize.RelTimeNWeatherElts;
import uwaterloo.mdc.etl.Discretize.VisitWithReadingEnum;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.model.UserVisitHierarchy;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

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
	protected final Frequency visitNoWLANFreq = new Frequency();
	protected final Frequency WLANNoVisitFreq = new Frequency();
	protected final SummaryStatistics durationStats = new SummaryStatistics();
	protected final Frequency durationFreqs = new Frequency();
//	protected final Frequency locationsPerUser = new Frequency();
//	protected final Frequency meaningsPerUser = new Frequency();
	

	protected long prevTime = -1;
	protected long prevprevtime;
	protected long currTime;
	protected File currStartDir;
	protected File prevStartDir;

	// It seems unuseful: protected long recordDeltaT;
	protected HashMap<String, Integer> prevAccessPoints = null;
	protected HashMap<String, Integer> currAccessPoints;
	protected String currMacAddr = null;

	// This is temporary, not part of the result
	protected SummaryStatistics apsStat = new SummaryStatistics();
	protected Frequency frequentlySeenAps = new Frequency();

	protected HashMap<String, LinkedList<KeyValuePair<String, String>>> userHierarchy = new HashMap<String, LinkedList<KeyValuePair<String, String>>>();
	protected LinkedList<KeyValuePair<String, String>> noVisitHierarchy = new LinkedList<KeyValuePair<String, String>>();

	protected Pattern tabSplit = Pattern.compile("\\t");
	protected long pendingEndTims = -1;
	protected File pendingVisitDir;
	protected SummaryStatistics pendingStats;

	protected final UserVisitHierarchy userVisits;

	@SuppressWarnings("deprecation")
	public RefineDocumentsFromWlan(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		relTimeWStats = new Frequency[RelTimeNWeatherElts.values().length];
		for (RelTimeNWeatherElts ix : RelTimeNWeatherElts.values()) {
			relTimeWStats[ix.ordinal()] = new Frequency();
		}
		userVisits = new UserVisitHierarchy(FileUtils.getFile(outPath, dataFile
				.getParentFile().getName()));
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
				prevStartDir = currStartDir;
				currStartDir = userVisits.getVisitDirForEndTime(currTime);
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

		assert prevTime != -1;

		long delta;

		KeyValuePair<String, String> docEndFile = null;
		LinkedList<KeyValuePair<String, String>> visitHierarchy = null;
		int newDocIx = -1;

		boolean significantChangeInLoc;
		// Determine significant changes from Visits
		if (currStartDir == null && prevStartDir == null) {
			significantChangeInLoc = false;
		} else if (currStartDir != null && prevStartDir != null) {
			significantChangeInLoc = !(currStartDir.getName()
					.equals(prevStartDir.getName()));
		} else {
			// one is null the other is not
			significantChangeInLoc = true;
		}

		if (significantChangeInLoc) {
			// Change on the Visit level
			forceStatsWrite();
			// Already happens inside force
			// //We are now tracking a new microlocation
			// apsStat = new SummaryStatistics();
		} else {
			// Try to refine from WiFi
			long macAddressesDistance = Discretize.getRxDistance(
					currAccessPoints, prevAccessPoints);
			// TODO: Consider using a Sigmoid function instead of simple
			// TODO: do we need to do anything with recordDelta?
			significantChangeInLoc = (macAddressesDistance >= Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD);
		}

		if (significantChangeInLoc) {
			// FilenameFilter timeFilter = new TimeFilenameFilter(prevTime);
			// File userDir = FileUtils.getFile(outPath, userid);
			// for (File visitDir : userDir.listFiles(timeFilter)) {
			// int visitStartTime = Integer.parseInt(StringUtils
			// .removeLastNChars(visitDir.getName(), 1));
			// if (visitStartTime > prevTime) {
			// break;
			// } else {
			// docStartDir = visitDir;
			// }
			// }
			//
			if (prevStartDir != null) {
				visitHierarchy = userHierarchy.get(prevStartDir.getName());
				if (visitHierarchy == null) {
					// Lazy init
					visitHierarchy = new LinkedList<KeyValuePair<String, String>>();
					userHierarchy.put(prevStartDir.getName(), visitHierarchy);

					File[] microLocFiles = prevStartDir.listFiles();
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
					locEndTime += Discretize.getStartEndTimeError(StringUtils
							.charAtFromEnd(visit.getKey(), 5));
					if (locEndTime < prevTime) {
						break;
					} else {
						docEndFile = visit;
						// The new doc will be for a time slot that is
						// before
						// the end of the visit, so it should be
						// placed after in the descending list
						++newDocIx;
					}
				}
			}

			if (docEndFile == null) {
				// The end time of the visit was before the record time
				WLANNoVisitFreq.addValue(ReadingWithinVisitEnum.R);

				// We are now tracking a new microlocation
				forceStatsWrite();

				// Don't even add this reading to stats
				return;
			} // else {
			WLANNoVisitFreq.addValue(ReadingWithinVisitEnum.B);

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

			// But this will cause a BLAH.. I dunno!
			// // We also provision for this happening when curr is
			// // processed, thus the check for end-curr
			// // Sooner or later, in both cases, force will be used
			// && (visitEndTime - currTime >= Config.WLAN_DELTAT_MIN)
			if ((visitEndTime - prevTime >= Config.WLAN_DELTAT_MIN)) {

				long microlocStartTime = prevprevtime;
				if (microlocStartTime == -1) {
					String visitStartDirName = prevStartDir.getName();
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
								+ prevStartDir.getAbsolutePath()
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
								.getStandardDeviation())));

				doc1.append('\t').append(consumeFrequentlySeenMacAddrs());

				doc1.append('\t')
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
				// frequentlySeenAps = new Frequency();

				// There is always something pending.. that's right!
			}
			// And in case this is the last microlocation
			// in the visit, but the time stayed there
			// is longer than WiFi sensing time, we have
			// to force the addition of the stats, in case
			// it doesn't get naturally added.
			// pendingEndTims = visitEndTime;
			// } else {
			// We have pending statistics that are not written to the
			// microlocation document. Keep track of them, and next
			// iteration force will be called if needed.
			pendingEndTims = visitEndTime;
			pendingVisitDir = prevStartDir;
			pendingStats = apsStat;

			// Always something pending }

		}

		apsStat.addValue(currAccessPoints.size());
		for (String macAddr : currAccessPoints.keySet()) {
			frequentlySeenAps.addValue(macAddr);
		}
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
		long lastEndTime = Long.parseLong(StringUtils.removeLastNChars(
				lastTimeSlot.getKey(), 5));

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
							1)).append('\t').append(lastEndTime).append('\t')
					.append(mean).append('\t').append(stder);
		} else if (docFields.length == 3) {
			// has already updated the time.. leave time itact
			doc.append(docFields[0]).append('\t').append(docFields[1])
					.append('\t').append(docFields[2]).append('\t')
					.append(mean).append('\t').append(stder);
		} else if (docFields.length == 10 && pendingEndTims == lastEndTime) {
			// This is the case when the document was already updated
			// because of an earlier change of mac address, but then
			// the last few readings needs some place to go. (Why?)

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

			// For the next visit
			frequentlySeenAps = new Frequency();
			apsStat = new SummaryStatistics();
			pendingEndTims = -1;

			return;
		}

		doc.append('\t').append(consumeFrequentlySeenMacAddrs());

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
		// frequentlySeenAps = new Frequency();
		apsStat = new SummaryStatistics();
		pendingEndTims = -1;
	}

	private String consumeFrequentlySeenMacAddrs() {
		StringBuilder result = new StringBuilder();

		Iterator<Comparable<?>> macIter = frequentlySeenAps.valuesIterator();
		int m = 0;
		while (macIter.hasNext() && m < Config.NUM_FREQ_MAC_ADDRS_TO_KEEP) {
			++m;
			result.append(" M").append(macIter.next().toString());
		}

		frequentlySeenAps = new Frequency();

		return result.toString();
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
				+ Config.DELIMITER_START_ENDTIME + "%s\t%s\t%s %s";
		String wifiReadingFormat = COLNAME_AVG_NUM_APS
				+ Config.DELIMITER_COLNAME_VALUE + "%s"
				+ COLNAME_STDDEV_NUM_APS + Config.DELIMITER_COLNAME_VALUE
				+ "%s";
		String relTimeWeatherFormat = COLNAME_DAY_OF_WEEK
				+ Config.DELIMITER_COLNAME_VALUE + "%s" + COLNAME_HOUR_OF_DAY
				+ Config.DELIMITER_COLNAME_VALUE + "%s" + COLNAME_TEMPRATURE
				+ Config.DELIMITER_COLNAME_VALUE + "%s" + COLNAME_SKY
				+ Config.DELIMITER_COLNAME_VALUE + "%s";

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
				visitNoWLANFreq.addValue(VisitWithReadingEnum.V);
				
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
				Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(
						startTimeStr, Config.DEFAULT_TIME_ZONE);

				String relTimeWeather = String.format(relTimeWeatherFormat,
						relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
								.ordinal()],
						relTimeAndWeather[RelTimeNWeatherElts.SKY.ordinal()]);
				malletInst = String.format(malletInstFormat, startTimeStr,
						endTimeStr, microLocInst, "", relTimeWeather);
				// No clutter:
				// Config.MISSING_VALUE_PLACEHOLDER,
				// Config.MISSING_VALUE_PLACEHOLDER,

				startTime = Long.parseLong(startTimeStr);
				endTime = Long.parseLong(endTimeStr);

				writeMallet(startTime, endTime, malletInst, FileUtils.getFile(
						malletDir, visitDir.getName(), microLocFile.getName()));

			} else {
				visitNoWLANFreq.addValue(VisitWithReadingEnum.B);
				for (KeyValuePair<String, String> microLocDoc : microLocList) {
					String[] instFields = tabSplit
							.split(microLocDoc.getValue());
					if (instFields.length == 10) {

						String wifi = String.format(wifiReadingFormat,
								instFields[3], instFields[4]);

						// frequenty seen macs
						wifi += instFields[5];

						String relTimeWeather = String.format(
								relTimeWeatherFormat, instFields[6],
								instFields[7], instFields[8], instFields[9]);

						malletInst = String.format(malletInstFormat,
								instFields[1], instFields[2], instFields[0],
								wifi, relTimeWeather);

						startTime = Long.parseLong(instFields[1]);
						// Long.parseLong(instFields[1].substring(0,
						// instFields[1].length() - 1));
						endTime = Long.parseLong(instFields[2]);
						// Long.parseLong(instFields[2].substring(0,
						// instFields[2].length() - 1));

						// FIXMED: Comment out! This case shouldn't happen any
						// more.. it was a bug
						// } else if (instFields.length == 3) {
						//
						// startTime = Long.parseLong(instFields[1]);
						// endTime = Long.parseLong(instFields[2]);
						// // Assume default timezone
						// Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(
						// startTime, Config.DEFAULT_TIME_ZONE);
						//
						// malletInst = String
						// .format(malletInstFormat,
						// instFields[1],
						// instFields[2],
						// instFields[0],
						// Config.MISSING_VALUE_PLACEHOLDER,
						// Config.MISSING_VALUE_PLACEHOLDER,
						// relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
						// .ordinal()],
						// relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
						// .ordinal()],
						// relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
						// .ordinal()],
						// relTimeAndWeather[RelTimeNWeatherElts.SKY
						// .ordinal()]);

					} else if (instFields.length == 1) {
						// This is the case of a file that was loaded into the
						// user hierarchy because there is a reading, but it
						// wasn't filled with data because the reading is
						// outside all
						// visits. So discard the reading.. no clutter!
						String startTimeStr = StringUtils.removeLastNChars(
								visitDir.getName(), 1);
						startTime = Long.parseLong(startTimeStr);
						String endTimeStr = StringUtils.removeLastNChars(
								microLocDoc.getKey(), 5);
						endTime = Long.parseLong(endTimeStr);

						// Assume default timezone
						Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(
								startTime, Config.DEFAULT_TIME_ZONE);

						String relTimeWeather = String
								.format(relTimeWeatherFormat,
										relTimeAndWeather[RelTimeNWeatherElts.DAY_OF_WEEK
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.HOUR_OF_DAY
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.TEMPRATURE
												.ordinal()],
										relTimeAndWeather[RelTimeNWeatherElts.SKY
												.ordinal()]);
						malletInst = String.format(malletInstFormat,
								startTimeStr, endTimeStr, instFields[0], "",
								relTimeWeather);
						// No clutter:
						// Config.MISSING_VALUE_PLACEHOLDER,
						// Config.MISSING_VALUE_PLACEHOLDER,

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
		result.put(Config.RESULT_KEY_VISIT_WLAN_BOTH_FREQ, visitNoWLANFreq);
		result.put(Config.RESULT_KEY_WLAN_VISIT_BOTH_FREQ, WLANNoVisitFreq);
		result.put(Config.RESULT_KEY_DURATION_FREQ, durationFreqs);
		result.put(Config.RESULT_KEY_DURATION_SUMMARY, durationStats);
		result.put(Config.RESULT_KEY_DAY_OF_WEEK_FREQ,
				relTimeWStats[RelTimeNWeatherElts.DAY_OF_WEEK.ordinal()]);
		result.put(Config.RESULT_KEY_HOUR_OF_DAY_FREQ,
				relTimeWStats[RelTimeNWeatherElts.HOUR_OF_DAY.ordinal()]);
		result.put(Config.RESULT_KEY_TEMPRATURE_FREQ,
				relTimeWStats[RelTimeNWeatherElts.TEMPRATURE.ordinal()]);
		result.put(Config.RESULT_KEY_SKY_FREQ,
				relTimeWStats[RelTimeNWeatherElts.SKY.ordinal()]);
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
