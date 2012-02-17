package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;

/**
 * Creates a hierarchy of users -> start times -> end times. Under each user a
 * folder will be created for the points of time that is believed to be the
 * start of an interesting period; a visit. Under this folder, one or more
 * folders will be created for each point in time when it is believed that a
 * transition in the micro-location has happened. The visits are as indicated in
 * visit_sequence_10min file (_20min is a subset), and the transitions in
 * micro-location are sensed from wlan changes.
 * 
 * All times are GMT times
 * 
 * @author yaboulna
 * 
 */
public class CreateDocumentsFromVisits extends CallableOperation<Void, Long> {
	public static final String TIMETRUSTED_FLAG_YES = "T";
	public static final String TIMETRUSTED_FLAG_NO = "U";
	public static final String DELIMITER_USER_FEATURE = "_";
	public static final String DELIMITER_START_ENDTIME = "-";
	public static final String DELIMITER_FILE_COLNAME = ".";
	public static final String DELIMITER_COLNAME_VALUE = "=";

	//
	// protected long recordStartTime = -1;
	// protected long recordEndTime = -1;

	@SuppressWarnings("deprecation")
	public CreateDocumentsFromVisits(ImportIntoMallet master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected void eoFileProcedure() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void headerDelimiterProcedure() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void delimiterProcedure() {
		try {
			Long longVal = new Long(currValue);
			colOpResult.put(currKey, longVal);
		} catch (NumberFormatException e) {
			colOpResult.put(currKey, -1L);
		}
	}

	@Override
	protected void eolProcedure() throws Exception {
		Long startTime = colOpResult.get("unixtime_start");
		String trustIndicator = TIMETRUSTED_FLAG_YES;
		if (colOpResult.get("trusted_start") == 0) {
			startTime = addError(startTime);
			trustIndicator = TIMETRUSTED_FLAG_NO;
		}
		String startTimeDirName = startTime.toString() + trustIndicator;

		Long endTime = colOpResult.get("unixtime_end");
		trustIndicator = TIMETRUSTED_FLAG_YES;
		if (colOpResult.get("trusted_end") == 0) {
			endTime = addError(endTime);
			trustIndicator = TIMETRUSTED_FLAG_NO;
		}
		String endTimeFileName = endTime.toString() + trustIndicator;

		// We keep the times in GMT, so no need to use tz

		String instanceName = userid + DELIMITER_USER_FEATURE
				+ startTimeDirName + DELIMITER_START_ENDTIME + endTimeFileName;

		endTimeFileName += ".csv";

		File visitFile = FileUtils.getFile(outPath, userid.toString(),
				startTimeDirName, endTimeFileName);

		long delta = System.currentTimeMillis();
		Writer visitWr = Channels.newWriter(
				FileUtils.openOutputStream(visitFile).getChannel(),
				Config.OUT_CHARSET);
		try {
			visitWr.append(instanceName).append('\t').append(userid)
					.append(DELIMITER_USER_FEATURE)
					.append(colOpResult.get("place_id").toString())
					.append('\t');
			// The place id is a per user id, so it is not a useful word. We use
			// it
			// as a label (for now)
			// .append("\tDUMMY\t")
			// .append("place_id")
			// .append(DELIMITER_COLNAME_VALUE)
			// .append(colOpResult.get("place_id").toString())
			// .append(" ");
			// NO, one instance per line: .append("\n")
		} finally {
			visitWr.flush();
			visitWr.close();
		}
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.IO_WRITE, delta);
		super.eolProcedure();
	}

	@Override
	protected void headerDelimiterProcedurePrep() {
		// Nothing

	}

	@Override
	protected void delimiterProcedurePrep() {
		// Nothing

	}

	@Override
	protected void writeResults() throws IOException {
		// Nothing

	}

	@Override
	protected String getHeaderLine() {
		// MALLEt doesn't use a header line
		// However the format is "name	label	data"
		// But if there is one instance per file:
		// MALLET will use the directory names as labels and the filenames as
		// instance names
		return "";
	}

	@Override
	protected Void getReturnValue() {
		// Nothing
		return null;
	}

	private long addError(long reading) {
		// TODO:Add a Gaussian Error of standard deviation of 10 minutes
		// since this time is not trusted; the start time is trusted if there
		// are location data points in the period of 10 minutes before the
		// arrival time (0=false, 1=true).

		return reading;
	}
}
