package uwaterloo.mdc.etl.util;

import java.io.File;
import java.io.FilenameFilter;

import uwaterloo.mdc.etl.Config;

public class TimeFilenameFilter implements FilenameFilter {

	// Find files for event within a day before the end time
	// (actually withing the last 99999 / 3600 = 27.7775 hours)
	String timePrefix;

	public TimeFilenameFilter(long endTime) {
		timePrefix = StringUtils.removeLastNChars(Long.toString(endTime),
				Config.TIME_SECONDS_IN_DAY_STRLEN);
	}

	@Override
	public boolean accept(File dir, String filename) {
		return filename.startsWith(timePrefix);
	}

}
