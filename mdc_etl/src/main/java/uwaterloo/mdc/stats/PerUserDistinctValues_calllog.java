package uwaterloo.mdc.stats;

import java.io.File;

public class PerUserDistinctValues_calllog extends PerUserDistinctValues {

	@Deprecated
	public PerUserDistinctValues_calllog(CalcPerUserStats master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected String getTimeColumnName() {
		return "call_time";
	}
}
