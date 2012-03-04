package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import uwaterloo.mdc.etl.Discretize;

public class LoadInputsIntoDocs_accel extends LoadInputsIntoDocs {

	
	public enum Movement {
		S, // Stationary
		D, // Device 
		U; // User
	}

	static {
		Discretize.enumsMap.put("accel_avdelt",
				LoadInputsIntoDocs_accel.Movement.values());
	}
	

	public LoadInputsIntoDocs_accel(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}
	
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("data");
		colsToSkip.add("stop");
	}

	@Override
	protected HashSet<String> getColsToSkip() {
		return colsToSkip;
	}


	@Override
	protected void delimiterProcedure() {
		// Timezone preceeds time only in this stupid case!
		if (getColsToSkip().contains(currKey)) {
			return;
		}

		if ("tz".equals(currKey)) {
			// We keep times in GMT..
			currTime = Long.parseLong(currValue);
		} else if (currKey.equals(getTimeColumnName())) {
			// calculateDeltaTime

			currTime += Long.parseLong(currValue);

			if (prevTimeColReading != null) {
				long deltaTime = currTime - prevTimeColReading;
				if (deltaTime != 0) {
					// We have finished readings for one time slot.. write
					// them
					onTimeChanged();
				}
			} 
			prevTimeColReading = currTime;
		} else {
			super.delimiterProcedure();
		}
	}

	@Override
	protected Comparable<?> getValueToWrite() {
		Enum<?> result = null;
		if ("avdelt".equals(currKey)) {
			Double avdelt = Double.parseDouble(currValue);
			if(avdelt < 100){
				result = Movement.S;
			} else if(avdelt < 1000){
				result = Movement.D;
			} else {
				result = Movement.U;
			}
		}
		return result;
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		if ("avdelt".equals(colName)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected String getTimeColumnName() {
		return "start";
	}
}
