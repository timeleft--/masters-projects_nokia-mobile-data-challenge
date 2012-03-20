package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_mediaplay extends PerUserQuantiles {

	public PerUserQuantiles_mediaplay(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// TODO Auto-generated constructor stub
	}
	
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("album");
		colsToSkip.add("artist");
		colsToSkip.add("track");
		colsToSkip.add("title");
		colsToSkip.add("uri");
		colsToSkip.add("duration");
		colsToSkip.add("state");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

}
