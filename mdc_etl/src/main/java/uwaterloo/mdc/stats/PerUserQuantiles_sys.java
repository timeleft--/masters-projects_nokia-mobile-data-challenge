package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashSet;

public class PerUserQuantiles_sys extends PerUserQuantiles {

	public PerUserQuantiles_sys(CalcQuantizationBoundaries master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		// TODO Auto-generated constructor stub
	}
	
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("profile");
		colsToSkip.add("freespace_c");
		colsToSkip.add("freespace_d");
		colsToSkip.add("freespace_e");
		colsToSkip.add("freespace_y");
		colsToSkip.add("freespace_z");
		colsToSkip.add("freeram");
		//Don't quantize nominals
		colsToSkip.add("charging");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

}
