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
		E, // eternal
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	};

	public enum PlaceLabelsEnum {
		Missing, // 0: Unknown
		H, // 1 : Home
		F, // 2 : Home of a friend, relative or colleague
		W, // 3 : My workplace/school
		T, // 4 : Location related to transportation (bus stop,  metro stop,  train station,  parking lot,  airport)
		Q, // 5 : The workplace/school of a friend, relative or colleague
		O,  // 6 : Place for outdoor sports (e.g. walking,  hiking,  skiing)
		I, // 7 : Place for indoor sports (e.g. gym)
		R, // 8 : Restaurant or bar
		S, // 9 : Shop or shopping center
		V; //10 : Holiday resort or vacation spot		
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	};

	public enum ReadingWithinVisitEnum {

//		V, // Should be counted alone
		R, // Reading with no Visit associated
		B, // Both
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	};
	
	public enum VisitWithReadingEnum {
		V, // Visit with no Reading associated
		B, // Both
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
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
				return super.toString();
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
				return super.toString();
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
				VisitWithReadingEnum.values());
		enumsMap.put(Config.RESULT_KEY_WLAN_VISIT_BOTH_FREQ,
				ReadingWithinVisitEnum.values());

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
		return Math.pow(10, (rx - 32.0) / 25.0); // Uses equation from
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
		timeZoneOffset = Integer.parseInt(timeZoneStr.substring(1)) / 3600;
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

	public static long getRxDistance(HashMap<String, Integer> currAccessPoints,
			HashMap<String, Integer> prevAccessPoints) {
		double displacementRelAp = 0;
		// long macAddressesDistance = 0;
		for (String mac : currAccessPoints.keySet()) {
			int currRSSI = currAccessPoints.get(mac);
			if (currRSSI > Config.WLAN_RSSI_MIN) {
				currRSSI = Config.WLAN_RSSI_MIN;
				;
			}
			Integer prevRSSI = prevAccessPoints.remove(mac);
			if (prevRSSI == null) {
				prevRSSI = Config.WLAN_RSSI_MIN;
			}
			displacementRelAp += Discretize.estimateApDistanceLaMarca(Math
					.abs(prevRSSI - currRSSI));

			// macAddressesDistance = macAddressesDistance + ((prevRSSI-currRSSI
			// ) ^ 2);
			// macAddressesDistance = macAddressesDistance +
			// Math.abs(prevRSSI-currRSSI);
		}
		for (Integer prevRssi : prevAccessPoints.values()) {
			// This AP is not visible any more, so add its RSSI
			displacementRelAp += Discretize.estimateApDistanceLaMarca(Math
					.abs((Config.WLAN_RSSI_MIN - prevRssi)));

			// macAddressesDistance = macAddressesDistance +
			// ((Config.WLAN_RSSI_MIN - prevRssi) ^ 2);
			// macAddressesDistance = macAddressesDistance +
			// Math.abs(Config.WLAN_RSSI_MIN - prevRssi);
		}

		// macAddressesDistance = Math.round(Math.log10(macAddressesDistance));
		// macAddressesDistance = Math.round(Math.sqrt(macAddressesDistance));

		// The macAddrDistance is the average of the displacement according to
		// each AP
		long macAddressesDistance = Math.round(displacementRelAp
				/ (currAccessPoints.size() + prevAccessPoints.size()));
		return macAddressesDistance;
	}

	public static Long getStartEndTimeError(char trust) {
		switch (trust) {
		case 'W':
			return 0L;
		case 'T':
			return Config.ERROR_START_END_TIMES;
		case 'U':
			return Config.TIME_SECONDS_IN_10MINS;
		default:
			return null;
		}
	}
}
