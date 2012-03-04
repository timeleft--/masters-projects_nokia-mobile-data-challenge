package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;

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
 * In the result, all times are GMT times 
 * 
 * @author yaboulna
 * 
 */
public class CreateDocumentsFromVisits extends CallableOperation<String, Long> {

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
		Long longVal = new Long(currValue);
		colOpResult.put(currKey, longVal);
	}

	@Override
	protected void eolProcedure() throws Exception {
		Long startTime = colOpResult.get("unixtime_start");
		// Times are already in GMT in this file only!!
//		 // Keep times in GMT
//		 startTime += colOpResult.get("tz_start");
		char trustIndicator = Config.TIMETRUSTED_GPS_YES;
		if (colOpResult.get("trusted_start") == 0) {
			trustIndicator = Config.TIMETRUSTED_GPS_NO;
		}
		String startTimeDirName = startTime.toString() + trustIndicator;

		Long endTime = colOpResult.get("unixtime_end");
		// Times are already in GMT in this file only!!
//		 // Keep times in GMT
//		 endTime += colOpResult.get("tz_end");
		trustIndicator = Config.TIMETRUSTED_GPS_YES;
		if (colOpResult.get("trusted_end") == 0) {
			trustIndicator = Config.TIMETRUSTED_GPS_NO;
		}
		String endTimeFileName = endTime.toString() + trustIndicator + ".csv";

		File visitFile = FileUtils.getFile(outPath, userid.toString(),
				startTimeDirName, endTimeFileName);

		long delta = System.currentTimeMillis();
		String userPlaceId = userid + Config.DELIMITER_USER_FEATURE
				+ colOpResult.get("place_id").toString();
		FileUtils.writeStringToFile(visitFile, userPlaceId, Config.OUT_CHARSET);
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
	protected String getReturnValue() {
		// Nothing
		return userid;
	}

}
