package uwaterloo.mdc.etl;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class Discretize {
	private Discretize() {
		// prevents init
	}

	public static Map<String, Enum<?>[]> enumsMap = Collections
			.synchronizedMap(new HashMap<String, Enum<?>[]>());

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
		E,// eternal
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString().substring(1);
			}
		};
	};

	public enum VisitReadingBothEnum {
		
		V, // FREQ_VISIT_NOWLAN_VAR
		R, // FREQ_NOVISIT_WLAN_VAR
		B, // FREQ_VISIT_WLAN_VAR
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString().substring(1);
			}
		};
	};

	public enum RelTimeNWeatherElts {
		DAY_OF_WEEK, HOUR_OF_DAY, TEMPRATURE, SKY
	};

	public enum DaysOfWeek {
		// Locale specific
		M, TU, W, TH, F, SA, SU;
	};

	public enum HourOfDay {
		H0, H1, H2, H3, H4, H5, H6, H7, H8, H9, H10, H11, H12, H13, H14, H15, H16, H17, H18, H19, H20, H21, H22, H23;
		public String toString() {
			return super.toString().substring(1);

		};

	};

	public enum Temprature {
		// TODO?
		F, // Freezing
		C, // Cold
		W, // Warm
		H, // Hot
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString().substring(1);
			}
		};
	};

	public enum Sky {
		
		// TODO
		S, // Sunny
		C, // Cloudy
		O, // Overcast
		D, // Drizzle
		R, // Raininng
		F, // Flurry
		X, // TODO Snow what??
		T, // Thunderstorm
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString().substring(1);
			}
		};
	};

	// Not comparable with other enums
	// public enum Missing{
	// placeHolder;
	// public String toString() {
	// return Config.MISSING_VALUE_PLACEHOLDER;
	// };
	// }

	static {
		enumsMap.put(Config.RESULT_KEY_DURATION_FREQ, DurationEunm.values());
		enumsMap.put(Config.RESULT_KEY_VISIT_WLAN_BOTH_FREQ,
				VisitReadingBothEnum.values());

		enumsMap.put(Config.RESULT_KEY_DAY_OF_WEEK_FREQ, DaysOfWeek.values());
		enumsMap.put(Config.RESULT_KEY_HOUR_OF_DAY_FREQ, HourOfDay.values());

		enumsMap.put(Config.RESULT_KEY_TEMPRATURE_FREQ, Temprature.values());
		enumsMap.put(Config.RESULT_KEY_SKY_FREQ, Sky.values());

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
		return rx / 2.5; // Uses equation from LaMarca et al.
	}

	public static double estimateApDistanceLaMarca(int rx) {
		// I removed the subtraction from 32 because I think that the rx is
		// already
		// normalized to start from 0.. if not, we might need to subtract from
		// it
		// the maximum and the 32: 110+32????? FIXME: what exactly is RX????
		return Math.pow(10, (rx/*-32.0*/) / 25.0); // Uses equation from
														// LaMarca et al.
	}

	public static Enum<?>[] relTimeNWeather(long startTime, String timeZoneStr) {
		Enum<?>[] result = new Enum[RelTimeNWeatherElts.values().length];

		char timeZonePlusMinus = '+';
		if (timeZoneStr.charAt(0) == '-') {
			timeZonePlusMinus = '-';
		}
		// Offset in hours (from seconds)
		int timeZoneOffset = 0;
		// try {
		timeZoneOffset = Integer.parseInt(timeZoneStr.substring(1)) / 3600;
		// } catch (NumberFormatException ex) {
		// // Ok calm down!
		// }
		TimeZone timeZone = TimeZone.getTimeZone("GMT" + timeZonePlusMinus
				+ timeZoneOffset);

		startTime = startTime * 1000;
		Calendar calendar = Calendar.getInstance(timeZone);
		calendar.setTimeInMillis(startTime);

		result[RelTimeNWeatherElts.HOUR_OF_DAY.ordinal()] = HourOfDay.values()[calendar
				.get(Calendar.HOUR_OF_DAY)];
		result[RelTimeNWeatherElts.DAY_OF_WEEK.ordinal()] = DaysOfWeek.values()[calendar
				.get(Calendar.DAY_OF_WEEK) - 1];

		// TODO get weather
		result[RelTimeNWeatherElts.TEMPRATURE.ordinal()] = Temprature.Missing;
		result[RelTimeNWeatherElts.SKY.ordinal()] = Sky.Missing;

		return result;
	}
}
