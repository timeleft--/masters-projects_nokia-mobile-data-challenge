package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;

public class RefineDocumentsFromWlan extends
		CallableOperation<KeyValuePair<String, Frequency>, String> {
	private static Writer LOG;

	protected class TimeFilter implements FilenameFilter {

		// Find files for visits within a day of the wifi ap reading
		// (actually withing the last 99999 / 3600 = 27.7775 hours)
		String timePrefix;

		public TimeFilter() {
			timePrefix = Long.toString(prevTime);
			timePrefix = timePrefix.substring(0, timePrefix.length()
					- Config.TIME_SECONDS_IN_DAY_STRLEN);
		}

		@Override
		public boolean accept(File dir, String filename) {
			return filename.startsWith(timePrefix);
		}

	}

	private static final String WLAN_NOVISITS_FILENAME = "wlan_no-visit.csv";

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

	@SuppressWarnings("deprecation")
	public RefineDocumentsFromWlan(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected void eoFileProcedure() throws Exception {
		// Process the last set of observations
		refineDocument(true);

		// And then do a check on all the files
		File userDir = FileUtils.getFile(outPath, userid);
		long delta;
		Pattern tabSplit = Pattern.compile("\\t");
		String malletInstFormat = userid + Config.DELIMITER_USER_FEATURE + "%s"
				+ Config.DELIMITER_START_ENDTIME + "%s\t%s\t"
				+ COLNAME_AVG_NUM_APS + "=%s" + COLNAME_STDDEV_NUM_APS + "=%s";

		for (File visitDir : userDir.listFiles()) {
			if(!visitDir.isDirectory()){
				continue;
			}
			for (File microLocFile : visitDir.listFiles()) {
				delta = System.currentTimeMillis();
				String microLocInst = FileUtils.readFileToString(microLocFile);
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_READ, delta);

				String malletInst;
				String[] instFields = tabSplit.split(microLocInst);
				long startTime;
				long endTime;
				if (instFields.length == 1) {
					malletInst = String.format(malletInstFormat, visitDir
							.getName(), FilenameUtils
							.removeExtension(microLocFile.getName()),
							instFields[0], "0", "0");
					visitNoVisitFreq.addValue(FREQ_VISIT_NOWLAN_VAR);
					startTime = 0;
					endTime = 0;

					delta = System.currentTimeMillis();
					synchronized (LOG) {
						LOG.append(new SimpleDateFormat().format(new Date()))
								.append("\tWARNING\tVisit with no WLAN records: ")
								.append(microLocFile.getAbsolutePath())
								.append('\n');
					}
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
				} else if (instFields.length == 5) {
					malletInst = String.format(malletInstFormat, instFields[1],
							instFields[2], instFields[0], instFields[3],
							instFields[4]);
					startTime = Long.parseLong(instFields[1]);
//					Long.parseLong(instFields[1].substring(0,
//					instFields[1].length() - 1));
					endTime = Long.parseLong(instFields[2]);
//					Long.parseLong(instFields[2].substring(0,
//					instFields[2].length() - 1));
				} else {
					delta = System.currentTimeMillis();
					synchronized (LOG) {
						LOG.append(new SimpleDateFormat().format(new Date()))
								.append("\tERROR\tWrong number of columns in file: ")
								.append(microLocFile.getAbsolutePath())
								.append('\n');
					}
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
					continue;
				}

				// Calculate duration
				long durationInSec = endTime - startTime;

				// TODONE: do we need to discritize even more or less?
				char durDiscrete = Discretize.duration(durationInSec);
	
				malletInst += " dur" + Config.DELIMITER_COLNAME_VALUE
						+ durDiscrete;

				delta = System.currentTimeMillis();
				FileUtils.writeStringToFile(microLocFile, malletInst,
						Config.OUT_CHARSET);
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_WRITE, delta);
			}
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
					refineDocument(false);
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

	private void refineDocument(boolean force) throws IOException {
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

		File docEndFile = null;

		// TODO: Consider using a Sigmoid function instead of simple
		// TODO: do we need to do anything with recordDelta?
		if (force
				|| macAddressesDistance > Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {
			FilenameFilter timeFilter = new TimeFilter();

			File userDir = FileUtils.getFile(outPath, userid);
			File docStartDir = null;
			for (File visitDir : userDir.listFiles(timeFilter)) {
				int visitStartTime = Integer.parseInt(visitDir.getName()
						.substring(0, visitDir.getName().length() - 1));
				if (visitStartTime > prevTime) {
					break;
				} else {
					docStartDir = visitDir;
				}
			}

			if (docStartDir == null) {
				docEndFile = FileUtils.getFile(userDir, WLAN_NOVISITS_FILENAME);
				visitNoVisitFreq.addValue(FREQ_NOVISIT_WLAN_VAR);
			} else {
				File[] microLocFiles = docStartDir.listFiles();
				for (int i = microLocFiles.length - 1; i >= 0; --i) {
					int locEndTime = Integer.parseInt(microLocFiles[i]
							.getName().substring(0,
									microLocFiles[i].getName().length() - 5));
					if (locEndTime < prevTime) {
						break;
					} else {
						docEndFile = microLocFiles[i];
					}
				}
				if (docEndFile == null) {
					// The end time of the visit was before the record time
					docEndFile = FileUtils.getFile(userDir,
							WLAN_NOVISITS_FILENAME);
					visitNoVisitFreq.addValue(FREQ_NOVISIT_WLAN_VAR);
				} else {
					// TODO: Consider using a Sigmoid function instead of simple
					// thresholding
					if (macAddressesDistance > Config.WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD) {

						long microlocStartTime = prevprevtime;
						if (microlocStartTime == -1) {
							String visitStartDirName = docStartDir.getName();
							microlocStartTime = Long
									.parseLong(visitStartDirName.substring(0,
											visitStartDirName.length() - 1));
						}
						String visitEndTimeStr = FilenameUtils
								.removeExtension(docEndFile.getName());
						visitEndTimeStr = visitEndTimeStr.substring(0,
								visitEndTimeStr.length() - 1);
						long visitEndTime = Long.parseLong(visitEndTimeStr);

						// Do not split if the reading is towards the end of the
						// visit, because this will result in a fragmented
						// document whose WLAN readings are not specified
						// // We also provision for this happening when curr is
						// // processed, thus the check for end-curr
						// (endTime - currTime) > Config.WLAN_DELTAT_MAX)
						if (visitEndTime - prevTime > Config.WLAN_DELTAT_MAX) {

							delta = System.currentTimeMillis();
							String placeId = FileUtils
									.readFileToString(docEndFile);
							int tabIx = placeId.indexOf('\t');
							if (tabIx != -1) {
								// This is not the first time the visit is split
								placeId = placeId.substring(0, tabIx);
							}
							delta = System.currentTimeMillis() - delta;
							PerfMon.increment(TimeMetrics.IO_READ, delta);

							StringBuilder doc1 = new StringBuilder();
							doc1.append(placeId)
									.append('\t')
									.append(microlocStartTime)
									.append('\t')
									.append(prevTime)
									// .append(Config.TIMETRUSTED_WLAN)
									.append('\t')
									.append(Long.toString(Math.round(apsStat
											.getMean())))
									.append('\t')
									.append(Long.toString(Math.round(apsStat
											.getStandardDeviation())));

							File doc1File = FileUtils
									.getFile(docStartDir, prevTime
											+ Config.TIMETRUSTED_WLAN + ".csv");
							delta = System.currentTimeMillis();
							FileUtils.writeStringToFile(doc1File,
									doc1.toString(), Config.OUT_CHARSET);
							delta = System.currentTimeMillis() - delta;
							PerfMon.increment(TimeMetrics.IO_WRITE, delta);

							StringBuilder doc2 = new StringBuilder();
							doc2.append(placeId).append('\t').append(currTime)
							// .append(Config.TIMETRUSTED_WLAN)
									.append('\t').append(visitEndTimeStr);

							delta = System.currentTimeMillis();
							FileUtils.writeStringToFile(docEndFile,
									doc2.toString(), Config.OUT_CHARSET);
							delta = System.currentTimeMillis() - delta;
							PerfMon.increment(TimeMetrics.IO_WRITE, delta);

							// We are now tracking a new microlocation
							apsStat = new SummaryStatistics();

						} else {
							// force appending the statistics to the file
							// (should be the last one)
							force = true;
						}

						if (visitEndTime - currTime < 0) {
							// This was the last reading for the current
							// visit, so force writing the stats
							force = true;
						}

						visitNoVisitFreq.addValue(FREQ_VISIT_WLAN_VAR);
					}
				}
			}
		}
		apsStat.addValue(currAccessPoints.size());

		if (force) {
			FileUtils.writeStringToFile(
					docEndFile,
					Long.toString(Math.round(apsStat.getMean()))
							+ "\t"
							+ Long.toString(Math.round(apsStat
									.getStandardDeviation())), true);

		}
		// We will not clutter the files with AP readings
		// // No need for synchronization, because there's one thread per user
		// Writer wr = Channels.newWriter(
		// FileUtils.openOutputStream(docEndFile, true).getChannel(),
		// Config.OUT_CHARSET);
		// delta = System.currentTimeMillis();
		// try {
		// wr.append("rec_t").append(Config.DELIMITER_COLNAME_VALUE)
		// .append(Long.toString(prevTime)).append(" delta_t")
		// .append(Config.DELIMITER_COLNAME_VALUE)
		// .append(Long.toString(recordDeltaT)).append(" rsl_diff")
		// .append(Config.DELIMITER_COLNAME_VALUE)
		// .append(Long.toString(macAddressesDistance)).append(" | ");
		// } finally {
		// wr.flush();
		// wr.close();
		//
		// delta = System.currentTimeMillis() - delta;
		// PerfMon.increment(TimeMetrics.IO_WRITE, delta);
		// }

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
		// Auto-generated method stub
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

}
