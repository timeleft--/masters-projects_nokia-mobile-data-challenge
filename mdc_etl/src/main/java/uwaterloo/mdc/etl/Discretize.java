package uwaterloo.mdc.etl;

import java.util.HashMap;

public class Discretize {
	private Discretize() {
		// prevents init
	}

	public static HashMap<String, Enum<?>[]> enumsMap = new HashMap<String, Enum<?>[]>();

	/**
	 * Keep enum values as one chars because they are written out to files and
	 * we want to minimize storage requirements
	 * 
	 * @author yaboulna
	 * 
	 */
	public enum DurationEunm {
		T, // tiny
		S, // short
		M, // medium
		L, // long
		H, // half working day
		W, // working day
		E;// epoch
	};

	public enum VisitReadingBothEnum {
		V, // FREQ_VISIT_NOWLAN_VAR
		R, // FREQ_NOVISIT_WLAN_VAR
		B; // FREQ_VISIT_WLAN_VAR
	};

	static {
		enumsMap.put(Config.RESULT_KEY_DURATION_FREQ, DurationEunm.values());
		enumsMap.put(Config.RESULT_KEY_VISIT_WLAN_BOTH_FREQ, VisitReadingBothEnum.values());
	}

	public static DurationEunm duration(long durationInSec) {
		long durationInMins = durationInSec / 60;
		DurationEunm durDiscrete;
		if (durationInMins < 10) {
			durDiscrete = DurationEunm.T;
		} else if (durationInMins <= 30) {
			durDiscrete = DurationEunm.S;
		} else if (durationInMins <= 60) {
			durDiscrete = DurationEunm.M;
		} else if (durationInMins <= 120) {
			durDiscrete = DurationEunm.L;
		} else if (durationInMins <= 240) {
			durDiscrete = DurationEunm.H;
		} else if (durationInMins <= 480) {
			durDiscrete = DurationEunm.W;
		} else {
			durDiscrete = DurationEunm.E;
		}
		return durDiscrete;

	}
	
	public static double estimateApDistanceLinear(int rx) {
		return rx/2.5; //Uses equation from LaMarca et al.
	}
	
	public static double estimateApDistanceLaMarca(int rx) {
		// I removed the subtraction from 32 because I think that the rx is already
		// normalized to start from 0.. if not, we might need to subtract from it
		// the maximum and the 32: 110+32????? FIXME: what exactly is RX????
		return Math.pow(10, (rx/*-32.0*/)/25.0); //Uses equation from LaMarca et al.
	}
}
