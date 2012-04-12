package uwaterloo.mdc.etl;

import java.util.Properties;

public class Config {
	private Config() {
		// Avoid creation
	}

	public static final String USERID_COLNAME = "userid";
	public static final int NUM_THREADS =8;
	public static final int NUM_USERS_TO_PROCESS = 80;
	public static final int VALIDATION_SAMPLE_USERS = 20;
	public static final int VALIDATION_FOLDS = 4; //Config.NUM_USERS_TO_PROCESS; 
	public static final int VALIDATION_FOLD_WIDTH = Config.NUM_USERS_TO_PROCESS / Config.VALIDATION_FOLDS; //8;


	
	public static final String LOG_PATH = "C:\\mdc-datasets\\mallet\\log";

	public static final String IN_CHARSET = "US-ASCII";
	public static final String OUT_CHARSET = "US-ASCII";
	
	public static final String MISSING_VALUE_PLACEHOLDER = "?";
	public static final Character MISSING_VALUE_PLACEHOLDER_CHAR = '?';


	public static final int NUMBER_TESTING_USERS = 20;

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

	public static final long TIME_SECONDS_IN_10MINS = 600;
	public static final long TIME_SECONDS_IN_HOUR = 60 * 60;
	public static final int TIME_SECONDS_IN_HOUR_STRLEN = Long.toString(
			Config.TIME_SECONDS_IN_HOUR).length();
	public static final long TIME_SECONDS_IN_DAY = 60 * 60 * 24;
	public static final long TIME_SECONDS_IN_WEEK =TIME_SECONDS_IN_DAY * 7;
	public static final long TIME_10MINS_IN_WEEK = Math.round(Math.ceil(TIME_SECONDS_IN_WEEK / TIME_SECONDS_IN_10MINS));
	public static final int TIME_SECONDS_IN_DAY_STRLEN = Long.toString(
			Config.TIME_SECONDS_IN_DAY).length();
	

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



	public static final boolean USER_SPECIFIC_FEATURES = false;

	// 0 is an extra label representing insignificant places
	public static final String[] LABELS_SINGLES = {"0","1","2","3","4","5","6","7","8","9","10"};
	public static final String[] LABEL_HIERARCHY = {
//		// Known vs unknown
//		"+0+-1-2-3-4-5-6-7-8-9-10-",
//		// Prevalent vs not
//		"+1+2+3+-4-5-6-7-8-9-10-",
//		// This is redundant.. it is exactly the previoys one: "-1-2-3-+4+5+6+7+8+9+10+",
//		// Add this layer and see if two levels or three levels is better
//				"+1+2+-3-", //Homes vs Work
//				"+4+5+-1-2-3-6-7-8-9-10-", //On the way vs rest
//				"+6+7+-1-2-3-4-5-8-9-10-", // Sports vs rest
//				"+8+9+-1-2-3-4-5-6-7-10-", // Commercial vs rest
//		// Pair wise:
//		"+1+-2-","+1+-3-",/*"+2+-1-",*/"+2+-3-",/*"+3+-1-","+3+-2-",*/
//		// One against all (that still remain)
//		"+4+-5-6-7-8-9-10-","+5+-4-6-7-8-9-10-","+6+-4-5-7-8-9-10-",
//		"+7+-4-5-6-8-9-10-","+8+-4-5-6-7-9-10-","+9+-4-5-6-7-8-10-",
//		"+10+-4-5-6-7-8-9-",
//		// Those are wrong!
////		"+4+-1-2-3-5-6-7-8-9-10-","+5+-1-2-3-4-6-7-8-9-10-","+6+-1-2-3-4-5-7-8-9-10-",
////		"+7+-1-2-3-4-5-6-8-9-10-","+8+-1-2-3-4-5-6-7-9-10-","+9+-1-2-3-4-5-6-7-8-10-",
////		"+10+-1-2-3-4-5-6-7-8-9-",
////		
		
//		// Transductive
//		"+1+-5-6-7-8-9-10-4-2-3-","+2+-4-6-7-8-9-10-1-5-3-","+3+-4-5-7-8-9-10-1-2-6-",
//		"+4+-5-6-7-8-9-10-1-2-3-","+5+-4-6-7-8-9-10-1-2-3-","+6+-4-5-7-8-9-10-1-2-3-",
//		"+7+-4-5-6-8-9-10-1-2-3-","+8+-4-5-6-7-9-10-1-2-3-","+9+-4-5-6-7-8-10-1-2-3-",
//		"+10+-4-5-6-7-8-9-1-2-3-",
		};
	public static final String[] LABELS_BINARY = new String[] { "+1","-1"};
	public static final int LABLES_BINARY_POSITIVE_IX = 0;
	public static final int LABLES_BINARY_NEGATIVE_IX = 1;
	public static final String LABELS_MULTICLASS_NAME = "ALL";
	
//	private static final String[] LABELS_CONSIDERED = LABEL_HIERARCHY;

	public static final int IO_BUFFER_SIZE = 64 * 1024;

	public static final int APP_USAGE_FREQ_PERCENTILE_MAX = 90;
	public static final int APP_USAGE_FREQ_PERCENTILE_MIN = 25;

	// The label from previous visit will be kept as a feature 
	// as long as the previous visit ended within these secs
	public static final long INTERVAL_LABEL_CARRY_OVER = 7200;

	public static final int NUM_QUANTILES = 4;

	public static final boolean QUANTIZATION_PER_USER = false;

	public static final boolean QUANTIZE_NOT_DISCRETIZE = false;

	public static final boolean WEKA_DISCRETIZE = true;

	public static final String PATH_PLACE_LABELS_PROPERTIES_FILE = "C:\\mdc-datasets\\place-labels.properties";
	public static final String QUANTIZED_FIELDS_PROPERTIES = "C:\\mdc-datasets\\numeric_quantized.properties";
	public static final String APPUID_PROPERTIES_FILE = "C:\\mdc-datasets\\app-uid_name.properties";

	public static final boolean SPREAD_NOMINAL_FEATURES_AS_BINARY = true;
	public static final boolean SPREAD_NOMINAL_FEATURES_USE_MISSING = false;
	public enum NORMALIZE_BY_ENUM {NONE, MAXIMUM, SUM};
	public static final NORMALIZE_BY_ENUM LOAD_NORMALIZE_BY = NORMALIZE_BY_ENUM.MAXIMUM;
	public static final boolean LOAD_MISSING_CLASS_AS_OTHER = true;
	public static final boolean LOADCOUNTS_DELETE_MISSING_CLASS = false;
	public static final boolean LOAD_REPLACE_MISSING_VALUES = true;
	public static final double LOAD_MISSING_VALUE_REPLA = 0.0; //Laplace = 1.0; regular = 0.0
	public static final boolean LOAD_FEATSELECTED_ONLY = true;
	public static final boolean LOAD_USER_SPECIFIC_FEATURE_FILTER = false;
	public static final boolean LOAD_DROP_VERYFREQUENT_VALS = true; //This is for apps and sie only
	public static final boolean LOAD_DROP_VERYRARE_VALS = true; // This is for apps onl
	public static final boolean LOAD_FEAT_SELECTED_APPS_ONLY = true;
	public static final boolean LOADCOUNT_APP_USAGE = true;
	public static final boolean LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER = false;
	public static final int LOAD_COUNT_SAMPLE_FIXED_NUMBER_FROM_USER_COUNT = 400;
	public static final boolean LOAD_NORMALIZE_VISIT_LENGTH = false;
	public static final boolean LOAD_USE_PLACEID = false;
	public static final boolean LOAD_WEIGHT_LABELS = false;
	public static final boolean LOAD_COUNT_WEIGHT = false;
	public static final boolean LOAD_WEIGHT_USERS = false;
	public static final boolean LOAD_COUNT_TRANSDUCTIVE_ZERO = false;
	
	public static final boolean LOADCOUNTS_FOR_SVMLIGHT_USING_SAVER = false;
	public static final boolean LOADCOUNTS_FOR_SVMLIGHT_MY_CODE = false;
	public static final boolean LOADCOUNTS_FOR_SVMLIGHT_TRANSDUCTIVE = false;
	public static final String SVMLIGHT_INPUTPATH = "C:\\mdc-datasets\\svmlight\\input";
	public static final String SVMLIGHT_OUTPUTPATH = "C:\\mdc-datasets\\svmlight\\output";
//	public static final boolean SVMLIGHT_TRAINED_CLASSIEFER = false;
	public static final boolean CLASSIFY_OUT_FOR_SVMLIGHT = true;

// These should be values per classifier type
//	public static final boolean CLASSIFY_USING_BIANRY_ENSEMBLE = true;
//	public static final boolean CLASSIFY_IGNORING_MISSING_LABELS = true;
//	public static final boolean CALSSIFYFEATSELECT_FEAT_SELECT = false;

	public static final String FEAT_SELECTED_APPS_PATH = "C:\\mdc-datasets\\feat-selected_apps.properties";

	public static final boolean CALC_ATTR_PAIRSWISE_CORRELATION = false;

	public static final boolean CALC_ATTR_PAIRSWISE_MUTUALINFO = true;

	public static final boolean RECORD_ONLY_ACCELOROMETER_CHANGES = true;
	public static final boolean RECORD_ONLY_USAGEFREQ_CHANGES = true;

	
	public static final boolean DROP_LOWEST_QUANTILE = false;

	public static final boolean DROP_HIGHEST_QUANTILE = false;

	public static final boolean MICROLOC_SPLITS_DOCS = false;

	public static final int CALCCUTPOINTS_NUM_SAMPLE_USERS = Config.NUM_USERS_TO_PROCESS / 2;
	
	public static final int CLUSTERCALSSIFY_LABEL_ASSG_MAX_ITERS = 30;
	public static final int CLUSTERCLASSIFY_NUM_CLUSTERS_MIN = 5;
	public static final int CLUSTERCLASSIFY_NUM_CLUSTERS_MAX = 11;
	public static final int CLUSTERCLASSIFY_NUM_KMEAN_RUNS = 7;
	public static final double CLASSIFY_CLUSTER_DEFINITIVE_PROB_DIV = 1.5;
	public static final double CLASSIFY_CLUSTER_SMALL_PROB_DIFF = 0.05;
	public enum CLUSTER_CLASSIFY_METRIC_ENUM {LIKELIHOOD, ENTROPY}; //, ERROR};
	public static final CLUSTER_CLASSIFY_METRIC_ENUM CLUSTER_CLASSIFY_METRIC = CLUSTER_CLASSIFY_METRIC_ENUM.LIKELIHOOD;
	public static final double CLUSTERCLASSIFY_EPSILON = 1e-6;
	public static final boolean CLUSTER_CLASSIFY_METRIC_IN_LOG_SPACE = true; //never change
	public static final boolean CLUSTERCLASSIFY_CLUSTER_IN_DIMREDUCED = true; //no reasong to change
	public static final boolean CLUSTERCLASSIFY_LAPALCAE_SMOOTH_LABEL_PROBS = true; //never change
	public static final boolean CLUSTER_CLASSIFY_REM_PREVLABEL = false;
	public static final boolean CLUSTER_CLASSIFY_PREVLABEL_DISTRIB = true;
	
	public static final boolean CLUSTER_IN3D_IN_TRANFORMED_SPACE = true;
	public static final boolean CLUSTER_IN3D_PER_USER = true;


	public static final boolean CLASSIFYFEATSEL_WRITE_TRANSFORMED = false;
	public static final boolean CLASSIFY_PENALIZE_ZERO = false;
	public static final boolean CLASSIFY_SEPARATE_PREVALENCE = false;
	public static final boolean CLASSIFY_USING_SAMPLE_FIXED_NUMBER_FROM_USER =false;
	public static final boolean CLASSIFY_FEAT_SELECTED_CLASSIFIER =true;
	
	
	
	// This class is thread-safe: multiple threads can share a single Properties
	// object without the need for external synchronization.
	public static Properties placeLabels;
	public static Properties quantizedFields;
	public static Properties appUidDictionary;

	
	public static String PATH_WEATHER= "D:\\datasets\\weather-underground";

	
	
}
