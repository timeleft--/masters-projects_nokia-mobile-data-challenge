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
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class RefineDocumentsFromWlan extends
		CallableOperation<KeyValuePair<String, Frequency>, String> {
	protected static Writer LOG;

	protected class TimeFilter implements FilenameFilter {

		// Find files for visits within a day of the wifi ap reading
		// (actually withing the last 99999 / 3600 = 27.7775 hours)
		String timePrefix;

		public TimeFilter() {
			timePrefix = StringUtils.removeLastNChars(Long.toString(prevTime),
					Config.TIME_SECONDS_IN_DAY_STRLEN);
			// timePrefix = timePrefix.substring(0, timePrefix.length()
			// - Config.TIME_SECONDS_IN_DAY_STRLEN);
		}

		@Override
		public boolean accept(File dir, String filename) {
			return filename.startsWith(timePrefix);
		}

	}

	// protected static final String WLAN_NOVISITS_FILENAME =
	// "wlan_no-visit.csv";

	protected static final String COLNAME_AVG_NUM_APS = " avgAps";
	protected static final String COLNAME_STDDEV_NUM_APS = " sdvAps";

	public static final char FREQ_NOVISIT_WLAN_VAR = 'w';
	public static final char FREQ_VISIT_WLAN_VAR = 'b';
	public static final char FREQ_VISIT_NOWLAN_VAR = 'v';

	protected long prevTime = -1;
	protected long prevprevtime;
	protected long currTime;
	// It seems unuseful: protected long recordDeltaT;
	protected HashMap<String, Integer> prevAccessPoints = null;
	protected HashMap<String, Integer> currAccessPoints;
	protected String currMacAddr = null;

	protected SummaryStatistics apsStat = new SummaryStatistics();
	protected Frequency visitNoVisitFreq = new Frequency();

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
	}

	@Override
	protected void eoFileProcedure() throws Exception {
		// Process the last set of observations
		forceStatsWrite(userHierarchy.get(pendingVisitDir.getName()),
					pendingVisitDir);
	
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
		} else if ("mac_address".equals(currKey)) {
			currMacAddr = currValue;

		} else if ("rx".equals(currKey)) {
			currAccessPoints.put(currMacAddr, new Integer(currValue));

		}

	}

	protected void refineDocument() throws IOException {
		long delta;
		long macAddressesDistance = 0; // The square difference between the
										// RSSIs of the same APs
		for (String mac : currAccessPoints.keySet()) {
			int currRSSI = currAccessPoints.get(mac);
			Integer prevRSSI = prevAccessPoints.remove(mac);
			if (prevRSSI == null) {
				prevRSSI = Config.WLAN_RSSI_MAX;
			}
			macAddressesDistance += (-currRSSI + prevRSSI) ^ 2;
		}
		for (Integer prevRssi : prevAccessPoints.values()) {
			// This AP is not visible any more, so add its RSSI
			macAddressesDistance += (Config.WLAN_RSSI_MAX - prevRssi) ^ 2;
		}

		macAddressesDistance = Math.round(Math.sqrt(macAddressesDistance));

		File docStartDir = null;
		// File docEndFile = null;
		KeyValuePair<String, String> docEndFile = null;
		LinkedList<KeyValuePair<String, String>> visitHierarchy = null;
		int newDocIx = -1;

		boolean force = false;

		// TODO: Consider using a Sigmoid function instead of simple
		// TODO: do we need to do anything with recordDelta?
		if (force
				|| macAddressesDistance > Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {
			FilenameFilter timeFilter = new TimeFilter();

			File userDir = FileUtils.getFile(outPath, userid);
			for (File visitDir : userDir.listFiles(timeFilter)) {
				int visitStartTime = Integer.parseInt(StringUtils
						.removeLastNChars(visitDir.getName(), 1));
				// visitDir.getName()
				// .substring(0, visitDir.getName().length() - 1));
				if (visitStartTime > prevTime) {
					break;
				} else {
					docStartDir = visitDir;
				}
			}

			// if (docStartDir == null) {
			// // docEndFile = FileUtils.getFile(userDir,
			// WLAN_NOVISITS_FILENAME);
			// docEndFile = new KeyValuePair<String,
			// String>(Long.toString(prevTime)+Config.TIMETRUSTED_WLAN, "");
			// noVisitHierarchy.add(docEndFile);
			// visitNoVisitFreq.addValue(FREQ_NOVISIT_WLAN_VAR);
			// } else {

			if (docStartDir != null) {
				//Always keep track of the last used dir
				pendingVisitDir = docStartDir;
				
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
					// visit.getKey().substring(
					// 0, visit.getKey().length() - 5));
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
				// docEndFile = FileUtils.getFile(userDir,
				// WLAN_NOVISITS_FILENAME);
				visitNoVisitFreq.addValue(FREQ_NOVISIT_WLAN_VAR);

				// We are now tracking a new microlocation
				pendingEndTims = -1;
				apsStat = new SummaryStatistics();
				return;

			}

			// TODO: Consider using a Sigmoid function instead of simple
			// thresholding
			if (macAddressesDistance > Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {

				long microlocStartTime = prevprevtime;
				if (microlocStartTime == -1) {
					String visitStartDirName = docStartDir.getName();
					microlocStartTime = Long.parseLong(StringUtils
							.removeLastNChars(visitStartDirName, 1));
					// visitStartDirName
					// .substring(0, visitStartDirName.length() - 1));
				}
				String visitEndTimeStr = StringUtils.removeLastNChars(
						docEndFile.getKey(), 5);
				// FilenameUtils.removeExtension();
				// visitEndTimeStr = visitEndTimeStr.substring(0,
				// visitEndTimeStr.length() - 1);

				long visitEndTime = Long.parseLong(visitEndTimeStr);
				if (pendingEndTims != -1 && pendingEndTims != visitEndTime) {
					// log("\tWARNING\tShould have appended the stat for visit ending: "
					// + pendingEndTims);
					forceStatsWrite(
							userHierarchy.get(pendingVisitDir.getName()),
							pendingVisitDir);
				}
				// Do not split if the reading is towards the end of the
				// visit, because this will result in a fragmented
				// document whose WLAN readings are not specified
				// We also provision for this happening when curr is
				// processed, thus the check for end-curr
				// Sooner or later, in both cases, force will be used
				if ((visitEndTime - prevTime >= Config.WLAN_DELTAT_MIN)
						&& (visitEndTime - currTime >= Config.WLAN_DELTAT_MIN)) {

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

					KeyValuePair<String, String> newDoc = new KeyValuePair<String, String>(
							Long.toString(prevTime) + Config.TIMETRUSTED_WLAN
									+ ".csv", doc1.toString());
					visitHierarchy.add(newDocIx, newDoc);
					// File doc1File = FileUtils.getFile(docStartDir, prevTime
					// + Config.TIMETRUSTED_WLAN + ".csv");
					// delta = System.currentTimeMillis();
					// FileUtils.writeStringToFile(doc1File, doc1.toString(),
					// Config.OUT_CHARSET);
					// delta = System.currentTimeMillis() - delta;
					// PerfMon.increment(TimeMetrics.IO_WRITE, delta);

					StringBuilder doc2 = new StringBuilder();
					doc2.append(placeId).append('\t').append(currTime)
							.append('\t').append(visitEndTimeStr);

					docEndFile.setValue(doc2.toString());
					// delta = System.currentTimeMillis();
					// FileUtils.writeStringToFile(docEndFile, doc2.toString(),
					// Config.OUT_CHARSET);
					// delta = System.currentTimeMillis() - delta;
					// PerfMon.increment(TimeMetrics.IO_WRITE, delta);

					// We are now tracking a new microlocation
					apsStat = new SummaryStatistics();
					pendingEndTims = -1;
				} else {
					// force "appending" the statistics to the file
					// (should be the last one)
					// This will happen next iteration anyway, when curr becomes
					// past visitEnd, right?
					// force = true;
					pendingEndTims = visitEndTime;
					pendingVisitDir = docStartDir;
					pendingStats = apsStat;

				}

				// This steals the readings from current, which it should enjoy
				// when it becomes prev!
				// if ((visitEndTime - currTime < Config.WLAN_DELTAT_MIN)
				// && (apsStat.getN() > 0)) {
				// // This was the last reading for the current
				// // visit, so force writing the stats
				// // only after the reading is added
				// // like any other reading
				// force = true;
				// }

				visitNoVisitFreq.addValue(FREQ_VISIT_WLAN_VAR);
			}

		}
		apsStat.addValue(currAccessPoints.size());

		// if (force) {
		// forceStatsWrite(visitHierarchy, docStartDir);
		// }
	}

	protected void forceStatsWrite(
			LinkedList<KeyValuePair<String, String>> visitHierarchy,
			File docStartDir) throws IOException {
		if (apsStat.getN() == 0) {
			// log("\tDEBUG\tCalling force for a stat with no datapoints, supposedly pending: "
			// + visitHierarchy.get(0));
			pendingEndTims = -1;
			return;
		}
		// FileUtils.writeStringToFile(
		// docEndFile,
		// Long.toString(Math.round(apsStat.getMean()))
		// + "\t"
		// + Long.toString(Math.round(apsStat
		// .getStandardDeviation())), true);
		// Force happens when we have to put the current stats in the last
		// time slot
		// last time slot is the first item in the descending list
		KeyValuePair<String, String> lastTimeSlot = visitHierarchy.get(0);

		if(StringUtils.removeLastNChars(lastTimeSlot.getKey(),
							4).endsWith("W")){
			log("\tDEBUG\tThis should end with either U or T: " + docStartDir.getAbsolutePath() + File.separator
					+ lastTimeSlot.getKey());
		}
		// stats
		String mean = Long.toString(Math.round(apsStat.getMean()));
		String stder = Long
				.toString(Math.round(apsStat.getStandardDeviation()));

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
				// stder = "?";
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
		File malletDir = FileUtils.getFile(outPath, "mallet", userid);
		String malletInstFormat = userid + Config.DELIMITER_USER_FEATURE + "%s"
				+ Config.DELIMITER_START_ENDTIME + "%s\t%s\t"
				+ COLNAME_AVG_NUM_APS + "=%s" + COLNAME_STDDEV_NUM_APS + "=%s";

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
				malletInst = String.format(malletInstFormat, startTimeStr,
						endTimeStr, microLocInst, "?", "?");
				visitNoVisitFreq.addValue(FREQ_VISIT_NOWLAN_VAR);
				startTime = Long.parseLong(startTimeStr);
				endTime = Long.parseLong(endTimeStr);

				writeMallet(startTime, endTime, malletInst, FileUtils.getFile(
						malletDir, visitDir.getName(), microLocFile.getName()));

			} else {
				for (KeyValuePair<String, String> microLocDoc : microLocList) {
					String[] instFields = tabSplit
							.split(microLocDoc.getValue());
					if (instFields.length == 5) {
						malletInst = String.format(malletInstFormat,
								instFields[1], instFields[2], instFields[0],
								instFields[3], instFields[4]);
						startTime = Long.parseLong(instFields[1]);
						// Long.parseLong(instFields[1].substring(0,
						// instFields[1].length() - 1));
						endTime = Long.parseLong(instFields[2]);
						// Long.parseLong(instFields[2].substring(0,
						// instFields[2].length() - 1));
					} else if (instFields.length == 3) {
						malletInst = String.format(malletInstFormat,
								instFields[1], instFields[2], instFields[0],
								"?", "?");
						startTime = Long.parseLong(instFields[1]);
						endTime = Long.parseLong(instFields[2]);
					} else if (instFields.length == 1) {
						// FIXME: This shouldn't happen.. but DUH!!!! it does!
						String startTimeStr = StringUtils.removeLastNChars(
								visitDir.getName(), 1);
						startTime = Long.parseLong(startTimeStr);
						String endTimeStr = StringUtils.removeLastNChars(
								microLocDoc.getKey(), 5);
						endTime = Long.parseLong(endTimeStr);
						malletInst = String.format(malletInstFormat,
								startTimeStr, endTime, instFields[0], "?", "?");
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

	protected void writeMallet(long startTime, long endTime, String malletInst,
			File malletFile) throws IOException {
		// Calculate duration
		long durationInSec = endTime - startTime;

		// TODONE: do we need to discritize even more or less?
		char durDiscrete = Discretize.duration(durationInSec);

		malletInst += " dur" + Config.DELIMITER_COLNAME_VALUE + durDiscrete;

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
	protected KeyValuePair<String, Frequency> getReturnValue() throws Exception {
		return new KeyValuePair<String, Frequency>(userid, visitNoVisitFreq);
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
