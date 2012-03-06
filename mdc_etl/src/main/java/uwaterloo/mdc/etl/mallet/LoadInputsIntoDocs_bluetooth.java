package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

public class LoadInputsIntoDocs_bluetooth extends LoadInputsIntoDocs {

	public LoadInputsIntoDocs_bluetooth(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}
	
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		// TODO: Remove when supporting OUI 
		colsToSkip.add("mac_prefix");
		colsToSkip.add("name");
	}
	
	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}


	@Override
	protected Comparable<?> getValueToWrite() {
		return "B" + currValue.toString(); //Mac Address
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

}
