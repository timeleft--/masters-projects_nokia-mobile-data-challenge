package uwaterloo.mdc.stats;

import java.io.File;

public class PerUserDistinctValues_gsm extends PerUserDistinctValues {
	
	@Deprecated
	public PerUserDistinctValues_gsm(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}
	
	@Override
	protected void delimiterProcedure() {
		if("signaldbm".equals(currKey)){
			//skip it, don't count its values
		} else {
			super.delimiterProcedure();
		}
	}

}
