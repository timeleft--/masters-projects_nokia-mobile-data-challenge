package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;

public class PerUserDistinctValues_calllog extends PerUserDistinctValues {

	@Deprecated
	public PerUserDistinctValues_calllog(CalcPerUserStats master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws IOException {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected String getTimeColumnName() {
		return "call_time";
	}
}
