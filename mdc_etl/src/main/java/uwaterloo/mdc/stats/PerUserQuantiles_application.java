package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_application extends PerUserQuantiles {

	public PerUserQuantiles_application(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// TODO Auto-generated constructor stub
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

}
