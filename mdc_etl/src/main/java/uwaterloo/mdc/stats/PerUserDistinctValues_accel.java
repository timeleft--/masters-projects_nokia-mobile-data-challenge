package uwaterloo.mdc.stats;

import java.io.File;


public class PerUserDistinctValues_accel extends PerUserDistinctValues {

	public static String ACCEL_DATA_COLNAME = "data";
	
	@Deprecated
	public PerUserDistinctValues_accel(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
 
	}
	
	@Override
	protected void delimiterProcedure() {
		if(ACCEL_DATA_COLNAME.equals(currKey)){
			//skip it, don't count its values
		} else {
			super.delimiterProcedure();
		}
	}
	
	@Override
	protected void headerDelimiterProcedurePrep() {
		if("start".equals(currValue) || "stop".equals(currValue)){
			currValue += "_time";
		}
		super.headerDelimiterProcedurePrep();
	}

	@Override
	protected String getTimeColumnName() {
		return "start_time";
	}
}
