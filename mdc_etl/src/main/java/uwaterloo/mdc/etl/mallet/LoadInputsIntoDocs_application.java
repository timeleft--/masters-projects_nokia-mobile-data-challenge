package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.MathUtil;

public class LoadInputsIntoDocs_application extends LoadInputsIntoDocs {

	private Frequency appUsageFreq = new Frequency();
	@Override
	protected void onMicroLocChange() {
		appUsageFreq = new Frequency();
	}
	
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
	public HashSet<String> getColsToSkip() {
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
			
			appUsageFreq.addValue(currValue);
			long encounters = appUsageFreq.getCount(currValue);
			
			if(MathUtil.getPow2(encounters) <0){
				// In case of num > 1024, that's a stop word!
				return null;
			}
			long lgEnc = MathUtil.tf(encounters);
			
			return Long.toString(lgEnc) + " aid" + Config.DELIMITER_COLNAME_VALUE + currValue.toString(); // The UID
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
