package uwaterloo.mdc.etl;

public class Config {
	private Config() {
		// Avoid creation
	}

	public static final String LOG_PATH = "C:\\Users\\yaboulna\\mdc2\\log";

	public static final String IN_CHARSET = "US-ASCII";
	public static final String OUT_CHARSET = "US-ASCII";
	
	public static final char MISSING_VALUE_PLACEHOLDER = '?';

	public static final String USERID_COLNAME = "userid";
	public static final int NUM_THREADS = 4;

	public static final String DELIMITER_USER_FEATURE = "_";
	public static final String DELIMITER_START_ENDTIME = "-";
	public static final String DELIMITER_FILE_COLNAME = ".";
	public static final String DELIMITER_COLNAME_VALUE = ""; // Yes.. empty!!

	public static final char TIMETRUSTED_GPS_YES = 'T';
	public static final char TIMETRUSTED_GPS_NO = 'U';
	public static final char TIMETRUSTED_WLAN = 'W';

	public static final long WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD = 13;
	// public static final long WLAN_MICROLOCATION_RSSI_DIFF_MIN_THRESHOLD = 3;
	public static final int WLAN_RSSI_MAX = 110;
	// Sampling period between 60 and 900 seconds
	public static final long WLAN_DELTAT_MIN = 60;
	public static final int WLAN_DELTAT_MAX = 900;

	public static final long TIME_SECONDS_IN_HOUR = 60 * 60;
	public static final int TIME_SECONDS_IN_HOUR_STRLEN = Long.toString(
			Config.TIME_SECONDS_IN_HOUR).length();
	public static final long TIME_SECONDS_IN_DAY = 60 * 60 * 24;
	public static final int TIME_SECONDS_IN_DAY_STRLEN = Long.toString(
			Config.TIME_SECONDS_IN_DAY).length();
	public static final long TIME_SECONDS_IN_10MINS = 600;

	public static final String RESULT_KEY_VISIT_WLAN_BOTH_FREQ = "visit-wlan-both_freq";
	public static final String RESULT_KEY_DURATION_FREQ = "duration-lengths_freq";
	public static final String RESULT_KEY_DURATION_SUMMARY = "duration-lengths_summary";

}
