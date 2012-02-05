package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;


public class PerUserDistinctValues_accel extends PerUserDistinctValues {

	public static String ACCEL_DATA_COLNAME = "data";
	
	@Deprecated
	public PerUserDistinctValues_accel(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws IOException {
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

}
