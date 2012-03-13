package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_accel extends PerUserQuantiles {

	public PerUserQuantiles_accel(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("data");
		colsToSkip.add("start");
		colsToSkip.add("stop");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}
}
