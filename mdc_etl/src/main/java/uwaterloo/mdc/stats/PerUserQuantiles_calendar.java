package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_calendar extends PerUserQuantiles {

	public PerUserQuantiles_calendar(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// TODO Auto-generated constructor stub
	}
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("time");
		colsToSkip.add("uid");
		colsToSkip.add("status");
		// The location ID of a calendar entry will only
		// be redundant to Wifi Macs, if not misleading
		colsToSkip.add("location");
		colsToSkip.add("class");
		colsToSkip.add("last_mod");
		colsToSkip.add("title");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

}
