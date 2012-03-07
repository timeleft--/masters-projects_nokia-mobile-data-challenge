package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;

public class LoadInputsIntoDocs_sys extends LoadInputsIntoDocs {

	private HashMap<String,Comparable<?>> prevVal = new HashMap<String,Comparable<?>>();

	public LoadInputsIntoDocs_sys(Object master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
		//  Auto-generated constructor stub
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
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}
	
	public enum BatteryLevels {
		E, // empty (almost)
		L, // low
		G, // Good
		F, // full (almost)
		Missing; // Bad
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	}
	public enum ChargingState {
		N, // 0: charger not connected/uninitialized,
        C, // 1: device is charging
        F, // 4: charging completed
        B; // 5: charging continued after brief interruption
	}
	public enum UsageRate {
		A, //Actively using
		F, // Frequent usage
		I, // Intermittent usage
		S, // Spaced usage
		R; // Rare usage
	}
	public enum RingOrSilence{
		R, // Significant Audible sound produce
		S; // Silence or beep
	}
	static{
		Discretize.enumsMap.put("sys_battery", BatteryLevels.values());
		Discretize.enumsMap.put("sys_charging", ChargingState.values());
		Discretize.enumsMap.put("sys_inactive", UsageRate.values());
		Discretize.enumsMap.put("sys_ring", RingOrSilence.values());
	}

	@Override
	protected Comparable<?> getValueToWrite() {
		Comparable<?> result = null;
		if("battery".equals(currKey)){
			int batPcg = Integer.parseInt(currValue);
			if(batPcg > 100){
				result = BatteryLevels.Missing;
			} else if(batPcg > 80){
				result = BatteryLevels.F;
			} else if(batPcg > 30){
				result = BatteryLevels.G;
			} else if(batPcg > 10){
				result = BatteryLevels.L;
			} else {
				result = BatteryLevels.E;
			}
		} else if("charging".equals(currKey)) {
			if("0".equals(currValue)){
				result = ChargingState.N;
			} else if("1".equals(currValue)){
				result = ChargingState.C;
			} else if("4".equals(currValue)){
				result = ChargingState.F;
			} else if("5".equals(currValue)){
				result = ChargingState.B;
			} 
		} else if("inactive".equals(currKey)) {
			int period = Integer.parseInt(currValue);
			if(period < 20){
				result = UsageRate.A;
			} else if (period < 120) {
				result = UsageRate.F;
			} else if (period < Config.TIME_SECONDS_IN_10MINS) {
				result = UsageRate.I;
			} else if (period < Config.TIME_SECONDS_IN_HOUR) {
				result = UsageRate.S;
			} else {
				result = UsageRate.R;
			}
		} else if("ring".equals(currKey)) {
			if("normal".equals(currValue)
				|| "ascending".equals(currValue)
				|| "ring_once".equals(currValue)){
				result = RingOrSilence.R;
			} else if("beep".equals(currValue)
					|| "silent".equals(currValue)) {
				result = RingOrSilence.S;
			}
		}
		if (prevVal.containsKey(currKey) && result.equals(prevVal.get(currKey))) {
			// prevent repeating the values of high granuality files
			//TODO: how will this affect stats?
			result = null;
		} else {
			prevVal.put(currKey, result);
		}
		
		return result;
	}
	
	@Override
	protected void onMicroLocChange() {
		prevVal.clear();
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return "battery".equals(colName)
			|| "inactive".equals(colName);
	}

}
