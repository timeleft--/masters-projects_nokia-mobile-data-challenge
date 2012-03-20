package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.Discretize.DurationEunm;
import uwaterloo.mdc.etl.Discretize.PlaceLabelsEnum;
import uwaterloo.mdc.etl.Discretize.ReadingWithinVisitEnum;
import uwaterloo.mdc.etl.Discretize.RelTimeNWeatherElts;
import uwaterloo.mdc.etl.Discretize.VisitWithReadingEnum;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.model.UserVisitHierarchy;
import uwaterloo.mdc.etl.model.Visit;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.FileStringPair;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;
import uwaterloo.mdc.etl.util.StringUtils;

public class RefineDocumentsFromWlan
		extends
		CallableOperation<KeyValuePair<String, HashMap<String, Object>>, String> {
	protected static Writer LOG;

	// protected static final String WLAN_NOVISITS_FILENAME =
	// "wlan_no-visit.csv";

	protected static final String COLNAME_AVG_NUM_APS = " avgaps";
	protected static final String COLNAME_STDDEV_NUM_APS = " sdvaps";

	// protected static final String READINGS_AT_SAME_TIME =
	// "TIME_CARDINALITY_";
	protected static final String COLNAME_HOUR_OF_DAY = " tod";
	protected static final String COLNAME_DAY_OF_WEEK = " dow";

	protected static final String COLNAME_TEMPRATURE = " tmp";
	protected static final String COLNAME_SKY = " sky";

	// protected static final String COLNAME_PLACE_MEANING = " place";

	private final Frequency[] relTimeWStats;
	protected final Frequency visitWithWLANFreq = new Frequency();
	protected final Frequency WLANWithinVisitFreq = new Frequency();
	protected final SummaryStatistics durationStats = new SummaryStatistics();
	protected final Frequency durationFreqs = new Frequency();
	protected final Frequency locationsPerUser = new Frequency();
	protected final Frequency meaningsPerUser = new Frequency();
	protected final Frequency numMicroLocsFreq = new Frequency();
	protected final Frequency avgApsFreq = new Frequency();

	protected long prevTime = -1;
	protected long prevprevtime;
	protected long currTime;
	protected Visit<FileStringPair> currVisit;
	protected Visit<FileStringPair> prevVisit;
	protected File currStartDir;
	protected File prevStartDir;

	protected String prevTimeZone;

	// It seems unuseful: protected long recordDeltaT;
	protected LinkedList<HashMap<String, Integer>> prevAccessPointsHistory = new LinkedList<HashMap<String, Integer>>();
	protected HashMap<String, Integer> currAccessPoints;
	protected String currMacAddr = null;
	protected HashSet<String> currSSIDs;

	// This is temporary, not part of the result
	protected SummaryStatistics apsStat = new SummaryStatistics();
	protected Frequency frequentlySeenAps = new Frequency();
	protected SummaryStatistics ssidStats = new SummaryStatistics();
	
	protected Pattern tabSplit = Pattern.compile("\\t");
	protected long pendingEndTims = -1;

	protected Visit<FileStringPair> pendingVisit;
//	protected SummaryStatistics pendingStats;

	protected final UserVisitHierarchy userVisitHier;

	private HashMap<Long, Integer> nummlMap = new HashMap<Long, Integer>();

	@SuppressWarnings({ "deprecation" })
	public RefineDocumentsFromWlan(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		relTimeWStats = new Frequency[RelTimeNWeatherElts.values().length];
		for (RelTimeNWeatherElts ix : RelTimeNWeatherElts.values()) {
			relTimeWStats[ix.ordinal()] = new Frequency();
		}
		userVisitHier = new UserVisitHierarchy(FileUtils.getFile(outPath,
				dataFile.getParentFile().getName()));

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
		if ("tz".equals(currKey)) {
			prevTimeZone = currValue;
		} else if ("time".equals(currKey)) {
			// We'd better let the errors propagate: try {
			currTime = Long.parseLong(currValue);
			// } else if ("tz".equals(currKey)) {
			// // Act on time in GMT
			// currTime += Long.parseLong(currValue);
			if (currTime != prevTime) {
				// A new set of AP sightings (record)
				if (prevTime != -1 && !prevAccessPointsHistory.isEmpty()) {
					// This is not the first sighting in the file:
					// // 1) Update the time delta
					// recordDeltaT = currTime - prevTime;

					// 2) Act upon the difference between it and the prev
					if (refineDocument()) {
						// This indicates that the current readings should
						// be retained.. otherwise, discard
						apsStat.addValue(currAccessPoints.size());
						for (String macAddr : currAccessPoints.keySet()) {
							frequentlySeenAps.addValue(macAddr);
						}
						ssidStats.addValue(currSSIDs.size());
					}
				}

				// Make the current ones be the prev.s
				prevprevtime = prevTime;
				prevTime = currTime;
				// prevTimeZone = currValue;
				prevVisit = currVisit;
				prevStartDir = currStartDir;
				currVisit = userVisitHier.searchInVisit(currTime, false);
				if (currVisit != null) {
					currStartDir = currVisit.getValue().get(0).getValue()
							.getKey().getParentFile();
				} else {
					currStartDir = null;
				}
				if (currAccessPoints != null) {
					prevAccessPointsHistory.addLast(currAccessPoints);
				}
				currAccessPoints = new HashMap<String, Integer>();
				currSSIDs = new HashSet<String>();
			}
			// } catch (NumberFormatException ignored) {
			// // ok!
			// }

		} else if ("mac_address".equals(currKey)) {
			currMacAddr = currValue;
		} else if ("ssid".equals(currKey)) {
			currSSIDs.add(currValue);
			
		} else if ("rx".equals(currKey)) {
			if (currStartDir == null) {
				// store the reading only if it is within some visit
				WLANWithinVisitFreq.addValue(ReadingWithinVisitEnum.R);
			} else {
				WLANWithinVisitFreq.addValue(ReadingWithinVisitEnum.B);
				currAccessPoints.put(currMacAddr, new Integer(currValue));
			}
		}

	}

	protected boolean refineDocument() throws IOException {

		assert prevTime != -1;
		if (prevStartDir == null) {
			// DEBUG mesasge below shows that this happens when there is nothing
			// pending 99.99999% of the time
			// TODO: act when it happen when there is something pending
			if (pendingEndTims != -1) {
				log("\tERROR\tRecent readings were outside of visits - pendingEndTime: "
						+ pendingEndTims
						+ ", apStats.n: "
						+ apsStat.getN()
						+ ", frequentlySeen.uniqueCount: "
						+ frequentlySeenAps.getUniqueCount());
			}
			// The previous readings, if any are not part of any visit
			// discard them by returning false
			// But first prepare for the next visit
			frequentlySeenAps = new Frequency();
			apsStat = new SummaryStatistics();
			prevAccessPointsHistory.clear();
			pendingEndTims = -1;
			ssidStats = new SummaryStatistics();
			return false;
		}

		boolean significantChangeInLoc;
		// Determine significant changes from Visits
		if (currStartDir == null) {
			// one is null the other is not
			significantChangeInLoc = true; // false;
		} else {
			// if (currStartDir != null && prevStartDir != null) {
			significantChangeInLoc = !(currStartDir.getName()
					.equals(prevStartDir.getName()));
		}

		if (significantChangeInLoc) {
			// Change on the Visit level
			forceStatsWrite();
			// Already happens inside force
			// //We are now tracking a new microlocation
			// apsStat = new SummaryStatistics();
			// prevAccessPointsHistory.clear();
//			ssidStats = new SummaryStatistics();
			return true;
		} // else {
			// Try to refine from WiFi
		double macAddressesDistance = Discretize.getRxDistance(
				currAccessPoints, prevAccessPointsHistory.getFirst());
		if (macAddressesDistance >= Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {
			// This transition alone is enough to indicate a change in
			// microloc
			significantChangeInLoc = true;

		}
		// If none of them is >= MAX_THRESHOLD, then the avg will never be
		// greater!
		// for (HashMap<String, Integer> prevAccessPoints :
		// prevAccessPointsHistory) {
		// long distance = Discretize.getRxDistance(currAccessPoints,
		// prevAccessPoints);
		// if (distance >= Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {
		// // This transition alone is enough to indicate a change in
		// // microloc
		// significantChangeInLoc = true;
		// break;
		// }
		// macAddressesDistance += distance;
		// }
		//
		// if (!significantChangeInLoc) {
		// // No one transition was enough to indicate change in microloc
		// // So we need to see if the movement was all within a small
		// // area, or was along a trajectory: we take the avg distance
		// macAddressesDistance /= prevAccessPointsHistory.size();
		// // TODO: Consider using a Sigmoid function instead of simple
		// // TODO: do we need to do anything with recordDelta?
		// significantChangeInLoc = (macAddressesDistance >=
		// Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD);
		// }

		if (!significantChangeInLoc) {
			// keep tracking the readings... we are still
			// at the same micro location
			return true;
		} // else {
			// A change in microlocation happened

		FileStringPair microLocFile = userVisitHier.getDocForEndTime(prevTime);
		if (microLocFile == null) {
			// The end time of the visit was before the record time
			// WLANWithinVisitFreq.addValue(ReadingWithinVisitEnum.R);
			if (pendingEndTims != -1) {
				// Some readings were still pending from that visit
				forceStatsWrite();
			}
			// Prepare for the next visit
			frequentlySeenAps = new Frequency();
			apsStat = new SummaryStatistics();
			pendingEndTims = -1;
			prevAccessPointsHistory.clear();
			ssidStats = new SummaryStatistics();
			// Don't even add this reading to stats
			return false;
		} // else {
			// WLANWithinVisitFreq.addValue(ReadingWithinVisitEnum.B);

		String visitEndTimeStr = StringUtils.removeLastNChars(microLocFile
				.getKey().getName(), 5);

		long visitEndTime = Long.parseLong(visitEndTimeStr);
		if (pendingEndTims != -1 && pendingEndTims != visitEndTime) {
			// log("\tWARNING\tShould have appended the stat for visit ending: "
			// + pendingEndTims);
			forceStatsWrite();
		}

		String visitStartDirName = prevStartDir.getName();
		Long visitStartTime = Long.valueOf(StringUtils.removeLastNChars(
				visitStartDirName, 1));
		Integer visitNumMl = nummlMap.get(visitStartTime);
		if (visitNumMl == null) {
			visitNumMl = 1;
			// nummlMap.put(visitStartTime, visitNumMl);
		}
		++visitNumMl;
		nummlMap.put(visitStartTime, visitNumMl);

		// Do not split if the reading is towards the end of the
		// visit, because this will result in a fragmented
		// document whose WLAN readings are not specified
		if (Config.MICROLOC_SPLITS_DOCS
				&& (visitEndTime - prevTime >= Config.WLAN_DELTAT_MIN)) {

			long microlocStartTime = prevprevtime;
			if (microlocStartTime == -1) {
				// This prev time is the first
				microlocStartTime = visitStartTime;
			}

			String placeId = microLocFile.getValue();
			int tabIx = placeId.indexOf('\t');
			if (tabIx != -1) {
				// This is not the first time the visit is split
				placeId = placeId.substring(0, tabIx);
				if (placeId.indexOf('\t') != -1) {
					log("\tINFO\tOverriding stats for visit: "
							+ microLocFile.getKey().getAbsolutePath());
				}
			}

			Enum<?>[] relTimeAndWeather = getRelTimeAndWeather(prevTime,
					prevTimeZone);

			String doc1Key = Long.toString(prevTime) + Config.TIMETRUSTED_WLAN
					+ ".csv";
			long doc1EndTime = prevTime;
			long doc1StartTime = microlocStartTime;
			if (doc1StartTime >= doc1EndTime) {
				doc1StartTime -= Discretize.getStartEndTimeError(StringUtils
						.charAtFromEnd(prevStartDir.getName(), 1));
				// if (doc1StartTime >= doc1EndTime) {
				// log("\tWARNING\tThis split will be later discarded because previous time shouldn't belong to the visit ("
				// + prevStartDir.getAbsolutePath()
				// + File.separator
				// + doc1Key
				// + "), even with added error! PrevTime: "
				// + prevTime
				// + " - microLocStart: "
				// + microlocStartTime);
				// } else {
				// log("\tDEBUG\tThe previous time shouldn't have belonged to the visit ("
				// + prevStartDir.getAbsolutePath()
				// + File.separator
				// + doc1Key
				// + "), but we add some error! PrevTime: "
				// + prevTime
				// + " - microLocStart: "
				// + microlocStartTime);
				// }
			}

			StringBuilder doc1 = new StringBuilder();
			doc1.append(placeId)
					.append('\t')
					.append(doc1StartTime)
					.append('\t')
					.append(doc1EndTime)
					.append('\t')
					.append(Long.toString(Math.round(ssidStats.getMean()))) //apsStat.getMean())))
					.append('\t')
					.append(Long.toString(Math.round(ssidStats //apsStat
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
					.append(relTimeAndWeather[RelTimeNWeatherElts.SKY.ordinal()]);

			FileStringPair newDoc = new FileStringPair(FileUtils.getFile(
					prevStartDir, doc1Key), doc1.toString());
			userVisitHier.addMicroLoc(prevVisit, prevTime,
					Config.TIMETRUSTED_WLAN, newDoc);

			long doc2StartTime = currTime;
			long doc2EndTime = visitEndTime;
			if (doc2StartTime >= doc2EndTime) {
				long addedError = Discretize.getStartEndTimeError(StringUtils
						.charAtFromEnd(microLocFile.getKey().getName(), 5));
				if (doc2StartTime - doc2EndTime <= addedError) {
					doc2EndTime += addedError;
					// log("\tDEBUG\tThe current time shouldn't have belonged to the visit ("
					// + microLocFile.getKey().getAbsolutePath()
					// + "), but we add some error! Currtime: "
					// + currTime
					// + " - VisitEnd: " + visitEndTimeStr);
				} else {
					// This shouldn't happen because this will be a chang of
					// location on the visit level
					doc2StartTime = prevTime + 1;
					// However there won't be anything wrong.. next time
					// currStartDir will be different from prevStartDir
					// And thus the stats will not be forced into this
					// visit, but rather written into the next visit.
				}
			}

			StringBuilder doc2 = new StringBuilder();
			doc2.append(placeId).append('\t').append(doc2StartTime)
					.append('\t').append(doc2EndTime);

			microLocFile.setValue(doc2.toString());

			// We are now tracking a new microlocation
			apsStat = new SummaryStatistics();
			frequentlySeenAps = new Frequency();
			prevAccessPointsHistory.clear();
			ssidStats = new SummaryStatistics();
		}
		// There is always something pending.. that's right!
		// In case this is the last microlocation
		// in the visit, but the time stayed there
		// is longer than WiFi sensing time, we have
		// to force the addition of the stats, in case
		// it doesn't get naturally added.
		// To have pending statistics that are not written to the
		// microlocation document. Keep track of them, and next
		// iteration force will be called if needed.
		pendingEndTims = visitEndTime;
		pendingVisit = prevVisit;
//		pendingStats = apsStat;

		return true;
	}

	protected void forceStatsWrite() throws IOException {
		if (pendingEndTims == -1) {
			// in case this call is extra
			return;
		}
//		if (pendingStats == null
//				|| (/* pendingStats == apsStat && */pendingStats.getN() == 0)) {
//			if (apsStat == null
//					|| (/* pendingStats == apsStat && */apsStat.getN() == 0)) {
				if (ssidStats == null
						|| (/* pendingStats == apsStat && */ssidStats.getN() == 0)) {
			// log("\tDEBUG\tCalling force for a stat with no datapoints, supposedly pending: "
			// + visitHierarchy.get(0));
			pendingEndTims = -1;
			return;
		}
		// Force happens when we have to put the current stats in the last
		// time slot
		// last time slot is the first item in the descending list
		// KeyValuePair<String, String> lastTimeSlot = microLocDocList.get(0);
		FileStringPair lastTimeSlot = pendingVisit.getValue().get(0).getValue();
		long lastEndTime = Long.parseLong(StringUtils.removeLastNChars(
				lastTimeSlot.getKey().getName(), 5));

		// stats
		String mean = Long.toString(Math.round(ssidStats.getMean())); //apsStat//pendingStats.getMean()));
		String stder = Long.toString(Math.round(ssidStats //apsStat//pendingStats
				.getStandardDeviation()));

		StringBuilder doc = new StringBuilder();
		String[] docFields = tabSplit.split(lastTimeSlot.getValue());
		if (docFields.length == 1) {
			// only the place id.. visit not split at all
			doc.append(docFields[0]).append('\t').append(pendingVisit.getKey())
					.append('\t').append(lastEndTime).append('\t').append(mean)
					.append('\t').append(stder);
		} else if (docFields.length == 3) {
			// has already updated the time.. leave time itact
			doc.append(docFields[0]).append('\t').append(docFields[1])
					.append('\t').append(docFields[2]).append('\t')
					.append(mean).append('\t').append(stder);
		} else if (docFields.length == 10) {
			// This is the case when the document was already updated
			// because of an earlier change of mac address, but then
			// the last few readings needs some place to go. (Why?)
			log("\tINFO\tForced to override stats for visit: "
					+ lastTimeSlot.getKey().getAbsolutePath());
			if (pendingEndTims != lastEndTime) {
				log("\tERROR\tAlso overriding with data from a different end time. Expected "
						+ lastEndTime
						+ " but the values belong to "
						+ pendingEndTims);
			}
			// This doesn't happen any more anyway
//			if (apsStat != pendingStats) {
//				double avg = apsStat.getMean() * apsStat.getN()
//						+ pendingStats.getMean() * pendingStats.getN();
//				avg /= (apsStat.getN() + pendingStats.getN());
//
//				mean = Long.toString(Math.round(avg));
//
//				// We use the avg of the two, even though that's not right
//				double var = (pendingStats.getStandardDeviation() + apsStat
//						.getStandardDeviation()) / 2;
//				stder = Long.toString(Math.round(var));
//			}
			doc.append(docFields[0]).append('\t').append(docFields[1])
					.append('\t').append(docFields[2]).append('\t')
					.append(mean).append('\t').append(stder);
		} else {
			KeyValuePair<Long, FileStringPair> badMicroLoc = pendingVisit
					.getValue().remove(0);
			assert badMicroLoc.getValue() == lastTimeSlot;

			log("\tERROR\tRemoving a file with " + docFields.length
					+ " columns: "
					+ badMicroLoc.getValue().getKey().getAbsolutePath());

			deleteMicroLocFile(badMicroLoc.getValue().getKey());

			// For the next visit
			frequentlySeenAps = new Frequency();
			apsStat = new SummaryStatistics();
			pendingEndTims = -1;
			prevAccessPointsHistory.clear();
			ssidStats = new SummaryStatistics();

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
		frequentlySeenAps = new Frequency();
		apsStat = new SummaryStatistics();
		prevAccessPointsHistory.clear();
		pendingEndTims = -1;
	}

	private String consumeFrequentlySeenMacAddrs() {
		if (Config.USER_SPECIFIC_FEATURES) {
			StringBuilder result = new StringBuilder();

			Iterator<Comparable<?>> macIter = frequentlySeenAps
					.valuesIterator();
			int m = 0;
			while (macIter.hasNext() && m < Config.NUM_FREQ_MAC_ADDRS_TO_KEEP) {
				++m;
				result.append(" ap").append(macIter.next().toString());
			}

			// reducing side effects...the caller is responsible
			// frequentlySeenAps = new Frequency();

			return result.toString();
		} else {
			return "";
		}
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

		Iterator<KeyValuePair<Long, ArrayList<KeyValuePair<Long, FileStringPair>>>> visitsIter = userVisitHier
				.getVisitListIterator();
		while (visitsIter.hasNext()) {

			KeyValuePair<Long, ArrayList<KeyValuePair<Long, FileStringPair>>> visit = visitsIter
					.next();

			String malletInst;
			long startTime;
			long endTime;

			String locationId = null;

			ArrayList<KeyValuePair<Long, FileStringPair>> microLocList = visit
					.getValue();
			for (KeyValuePair<Long, FileStringPair> microLoc : microLocList) {
				FileStringPair microLocDoc = microLoc.getValue();
				String[] instFields = tabSplit.split(microLocDoc.getValue());
				if (instFields.length == 10) {

					visitWithWLANFreq.addValue(VisitWithReadingEnum.B);

					String wifi = String.format(wifiReadingFormat,
							instFields[3], instFields[4]);
					avgApsFreq.addValue(Integer.parseInt(instFields[3]));
					// frequenty seen macs
					wifi += instFields[5];

					String relTimeWeather = String.format(relTimeWeatherFormat,
							instFields[6], instFields[7], instFields[8],
							instFields[9]);

					malletInst = String.format(malletInstFormat, instFields[1],
							instFields[2], instFields[0], wifi, relTimeWeather);

					startTime = Long.parseLong(instFields[1]);

					endTime = Long.parseLong(instFields[2]);

				} else if (instFields.length == 3) {

					visitWithWLANFreq.addValue(VisitWithReadingEnum.B);
					// FIXME: This case shouldn't happen
					log("\tINFO\tFile with 3 columns: "
							+ microLocDoc.getKey().getAbsolutePath()
							+ " - Values: " + instFields[0] + "\t"
							+ instFields[1] + "\t" + instFields[2]);
					startTime = Long.parseLong(instFields[1]);
					endTime = Long.parseLong(instFields[2]);
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

					malletInst = String.format(malletInstFormat, instFields[1],
							instFields[2], instFields[0], "", relTimeWeather);

				} else if (instFields.length == 1) {

					visitWithWLANFreq.addValue(VisitWithReadingEnum.V);

					startTime = visit.getKey();

					String endTimeStr = StringUtils.removeLastNChars(
							microLocDoc.getKey().getName(), 5);
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
							Long.toString(startTime), endTimeStr,
							instFields[0], "", relTimeWeather);

				} else {
					File badFile = microLocDoc.getKey();
					log("\tERROR\tDiscarding file with " + instFields.length
							+ " columns: " + badFile.getAbsolutePath());
					deleteMicroLocFile(badFile);
					continue;
				}

				locationId = instFields[0];
				String visitName = Long.toString(visit.getKey())
						+ ((Visit<FileStringPair>) visit).trust;
				Integer numml = nummlMap.get(visit.getKey());
				if (numml == null) {
					numml = 0; // 0 not 1 so that it is not included as a
								// feature, later when loading 
				}
				writeMallet(startTime, endTime, malletInst, FileUtils.getFile(
						malletDir, visitName, microLocDoc.getKey().getName()),
						numml);
				// microLocList.size());

				locationsPerUser.addValue(locationId);

				String labelStr = Config.placeLabels.getProperty(locationId,
						"0");
				PlaceLabelsEnum meaning = PlaceLabelsEnum.values()[Integer
						.parseInt(labelStr)];
				meaningsPerUser.addValue(meaning);
				// I don't think the class should be part of the
				// if(!PlaceLabelsEnum.Missing.equals(meaning)){
				// malletInst += COLNAME_PLACE_MEANING + meaning;
				// }

			}
		}
	}

	// protected Enum<?>[] getRelTimeAndWeather(String startTimeStr,
	// String timeZoneStr) throws NumberFormatException, IOException {
	// return getRelTimeAndWeather(Long.parseLong(startTimeStr), timeZoneStr);
	// }

	protected Enum<?>[] getRelTimeAndWeather(long startTime, String timeZoneStr)
			throws IOException {
		Enum<?>[] result = Discretize.relTimeNWeather(startTime, timeZoneStr);
		// Config.DEFAULT_TIME_ZONE);

		for (RelTimeNWeatherElts ix : RelTimeNWeatherElts.values()) {
			relTimeWStats[ix.ordinal()].addValue(result[ix.ordinal()]);
		}

		return result;
	}

	protected void writeMallet(long startTime, long endTime, String malletInst,
			File malletFile, int numMicroLocs) throws IOException {
		// Calculate duration
		long durationInSec = endTime - startTime;

		if (durationInSec <= 0) {
			log("\tWARNING\tRemoving a file with negative duration ("
					+ durationInSec + " secs): " + malletFile.getAbsolutePath());
			deleteMicroLocFile(malletFile);
			return;
		}

		DurationEunm durDiscrete = Discretize.duration(durationInSec);

		durationStats.addValue(durationInSec);
		durationFreqs.addValue(durDiscrete);

		malletInst += " dur" + Config.DELIMITER_COLNAME_VALUE
				+ durDiscrete.toString();

		numMicroLocsFreq.addValue(numMicroLocs);
		if (numMicroLocs > 1) {
			malletInst += " numml" + MathUtil.tf(numMicroLocs);
		}

		long delta = System.currentTimeMillis();

		FileUtils.writeStringToFile(malletFile, malletInst, Config.OUT_CHARSET);
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.IO_WRITE, delta);
	}

	private void deleteMicroLocFile(File malletFile) throws IOException {
		File visitDir = malletFile.getParentFile();
		malletFile.delete();
		if (visitDir.listFiles() == null || visitDir.listFiles().length == 0) {
			log("\tINFO\tRemoving a visit dir with no more microlocations: "
					+ visitDir.getAbsolutePath());
			visitDir.delete();
		}
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
		result.put(Config.RESULT_KEY_VISIT_WLAN_BOTH_FREQ, visitWithWLANFreq);
		result.put(Config.RESULT_KEY_WLAN_VISIT_BOTH_FREQ, WLANWithinVisitFreq);
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
		result.put(Config.RESULT_KEY_LOCATIONS_PER_USER, locationsPerUser);
		result.put(Config.RESULT_KEY_MEANINGS_PER_USER, meaningsPerUser);
		result.put(Config.RESULT_KEY_NUM_MICRO_LOCS_FREQ, numMicroLocsFreq);
		result.put(Config.RESULT_KEY_AVG_APS_FREQ, avgApsFreq);
		// result.put(Config.RESULT_KEY_STDV_APS_??, meaningsPerUser);
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
