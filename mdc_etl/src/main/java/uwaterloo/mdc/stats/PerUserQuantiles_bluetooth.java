package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_bluetooth extends PerUserQuantiles {

	public PerUserQuantiles_bluetooth(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// TODO Auto-generated constructor stub
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

}
