package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;

public class LoadInputsIntoDocs_mediaplay extends LoadInputsIntoDocs {

	public LoadInputsIntoDocs_mediaplay(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		//  Auto-generated constructor stub
	}

	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("album");
		colsToSkip.add("artist");
		colsToSkip.add("track");
		colsToSkip.add("title");
		colsToSkip.add("uri");
		colsToSkip.add("duration");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}
	
	public enum PlayerState {
		Missing, //just a place holder
		P; //assuming all states are Playing
//		S1,
//		S2,
//		S3,
//		S4,
//		S5,
//		S6,
//		S7,
//		S8,
//		S9;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	};
	
	static {
		Discretize.enumsMap.put("mediaplay_state", PlayerState.values());
	}
	
	@Override
	protected Comparable<?> getValueToWrite() {
		// state
//		return PlayerState.values()[Integer.parseInt(currValue)];
		return PlayerState.P;
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

}
