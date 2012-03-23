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
		// Auto-generated constructor stub
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

	/**
	 * None: state unavailable / failure to read 0: not initialized 1:
	 * initializing 2: playing 3: paused 4: stopped 5: seeking (forward) 6:
	 * seeking (backward) 7: buffering 8: downloading
	 * 
	 * @author yaboulna
	 * 
	 */
	public enum PlayerState {
		Missing, // placeholder for historic reasons
		P, // Playing
		L; // Loading (online)
			// S1,
			// S2,
			// S3,
			// S4,
			// S5,
			// S6,
			// S7,
			// S8,
			// S9;
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
		// return PlayerState.values()[Integer.parseInt(currValue)];
		if ("2356".contains(currValue)) {
			return PlayerState.P;
		} else if ("78".contains(currValue)) {
			return PlayerState.L;
		} else {
			return null; // ignore other states
		}
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

}
