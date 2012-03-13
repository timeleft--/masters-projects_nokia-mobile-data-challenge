package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_calllog extends PerUserQuantiles {

	public PerUserQuantiles_calllog(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		//
	}

	@Override
	protected String getTimeColumnName() {
		return "call_time";
	}

	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		// We don't care about the status of sms
		colsToSkip.add("status");
		// And the region being called as well
		colsToSkip.add("number_prefix");
		// We also don't track repeated numbers (TODO: yet??)
		colsToSkip.add("number");

	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

}
