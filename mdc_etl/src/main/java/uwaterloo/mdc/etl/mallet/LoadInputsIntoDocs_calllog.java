package uwaterloo.mdc.etl.mallet;

import java.io.File;

public class LoadInputsIntoDocs_calllog extends LoadInputsIntoDocs {

	public LoadInputsIntoDocs_calllog(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}
	
	@Override
	protected String getTimeColumnName() {
		return "call_time";
	}

	@Override
	protected void delimiterProcedure() {
		
		if(currKey.equals(getTimeColumnName()) || "tz".equals(getTimeColumnName())){
			// Super will track stuff about time, this class tracks the rest
			super.delimiterProcedure();
		}
	}
	@Override
	protected String getValueToWrite() {
		String result;
		if("status".equals(currKey)){
			
		}
		
		
		return result;
	}

}
