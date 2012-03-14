package uwaterloo.mdc.etl;

import java.util.Properties;

public class Config {
	private Config() {
		// Avoid creation
	}

	public static final String LOG_PATH = "C:\\mdc-datasets\\mallet\\log";

	public static final String IN_CHARSET = "US-ASCII";
	public static final String OUT_CHARSET = "US-ASCII";
	
	public static final String MISSING_VALUE_PLACEHOLDER = "?";
	public static final Character MISSING_VALUE_PLACEHOLDER_CHAR = '?';

	public static final String USERID_COLNAME = "userid";
	public static final int NUM_THREADS = 32;
	public static final int NUM_USERS_TO_PROCESS = 80;

	public static final String DELIMITER_USER_FEATURE = "_";
	public static final String DELIMITER_START_ENDTIME = "-";
	public static final String DELIMITER_FILE_COLNAME = ".";
	public static final String DELIMITER_COLNAME_VALUE = ""; // Yes.. empty!!

	public static final char TIMETRUSTED_GPS_YES = 'T';
	public static final char TIMETRUSTED_GPS_NO = 'U';
	public static final char TIMETRUSTED_WLAN = 'W';
	public static final char TIMETRUSTED_ERROR = 'E';

	public static final long WLAN_MICROLOCATION_RSSI_DIFF_MAX_THRESHOLD = 10;
	// public static final long WLAN_MICROLOCATION_RSSI_DIFF_MIN_THRESHOLD = 3;
	public static final int WLAN_RSSI_MIN = 96; // according to an e-mail the max is 96 not 110;
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

	public static final String RESULT_KEY_READING_NOVISIT_FREQ = "reading-within-visit-freq";
	public static final String RESULT_KEY_VISIT_NOREADING_FREQ = "visit-with-reading-freq";

	public static final String DEFAULT_TIME_ZONE = "-7200";

	public static final String RESULT_POSTFX_INTEGER = "_int";

	public static final String RESULT_KEY_VISIT_WLAN_BOTH_FREQ = "visit-with-wlan_freq";
	public static final String RESULT_KEY_WLAN_VISIT_BOTH_FREQ = "wlan-with-visit_freq";
	public static final String RESULT_KEY_DURATION_FREQ = "microloc-duration_freq";
	public static final String RESULT_KEY_DURATION_SUMMARY = "microloc-duration_summary";
	public static final String RESULT_KEY_DAY_OF_WEEK_FREQ = "day-of-week_freq";
	public static final String RESULT_KEY_HOUR_OF_DAY_FREQ = "hour-of-day_freq";
	public static final String RESULT_KEY_TEMPRATURE_FREQ = "temprature_freq";
	public static final String RESULT_KEY_SKY_FREQ = "sky_freq";
	public static final String RESULT_KEY_LOCATIONS_PER_USER = "location-ids_freq";
	public static final String RESULT_KEY_MEANINGS_PER_USER = "location-meanings_freq";
	public static final String RESULT_KEY_NUM_MICRO_LOCS_FREQ = "microloc-per-visit_freq" + RESULT_POSTFX_INTEGER;
	public static final String RESULT_KEY_AVG_APS_FREQ = "microloc-wlan-aps-count_freq" + RESULT_POSTFX_INTEGER;
	public static final String RESULT_KEY_AVG_BTS_FREQ = "microloc-bluetooth-dev-avg_freq" + RESULT_POSTFX_INTEGER;


	public static final long ERROR_START_END_TIMES = 60 * 3;

	public static final int NUM_FREQ_MAC_ADDRS_TO_KEEP = 3;


	public static final int NUMBER_TESTING_USERS = 20;

	public static final int VALIDATION_FOLDS = Config.NUM_USERS_TO_PROCESS; //10;
	public static final int VALIDATION_FOLD_WIDTH = Config.NUM_USERS_TO_PROCESS / Config.VALIDATION_FOLDS; //8;

	public static final boolean USER_SPECIFIC_FEATURES = false;

	public static final int NUM_LABELS_CONSIDERED = 10;

	public static final String[] LABELS = {"1","2","3","4","5","6","7","8","9","10"};

	public static final int IO_BUFFER_SIZE = 64 * 1024;

	public static final int APP_USAGE_FREQ_PERCENTILE_MAX = 90;
	public static final int APP_USAGE_FREQ_PERCENTILE_MIN = 25;

	// The label from previous visit will be kept as a feature 
	// as long as the previous visit ended within these secs
	public static final long INTERVAL_LABEL_CARRY_OVER = 7200;

	public static final int NUM_QUANTILES = 4;

	public static final boolean QUANTIZATION_PER_USER = true;

	public static final boolean QUANTIZE_NOT_DISCRETIZE = true;

	public static final String PATH_PLACE_LABELS_PROPERTIES_FILE = "C:\\mdc-datasets\\place-labels.properties";
	public static final String QUANTIZED_FIELDS_PROPERTIES = "C:\\mdc-datasets\\numeric_quantized.properties";
	public static final String APPUID_PROPERTIES_FILE = "C:\\mdc-datasets\\app-uid_name.properties";

	public static /*final*/ boolean MICROLOC_SPLITS_DOCS = false;
	
	

	// This class is thread-safe: multiple threads can share a single Properties
	// object without the need for external synchronization.
	public static Properties placeLabels;
	public static Properties quantizedFields;
	public static Properties appUidDictionary;

	
	public static String PATH_WEATHER= "D:\\datasets\\weather-underground";

	
	
}
