package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import uwaterloo.mdc.etl.Config;

public class LoadInputsIntoDocs_application extends LoadInputsIntoDocs {

	public LoadInputsIntoDocs_application(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// Auto-generated constructor stub
	}

	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("event");
		colsToSkip.add("name");
	}

	@Override
	protected HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

	private boolean foregroundEvent = false;

	@Override
	protected void delimiterProcedure() {
		if ("event".equals(currKey)) {
			foregroundEvent = "Application.Foreground".equals(currValue);
		}
		super.delimiterProcedure();
	}

	@Override
	protected Comparable<?> getValueToWrite() {
		if (foregroundEvent) {
			return "A" + currValue; // The UID
		} else {
			return Config.MISSING_VALUE_PLACEHOLDER;
		}
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		// Auto-generated method stub
		return false;
	}

}
