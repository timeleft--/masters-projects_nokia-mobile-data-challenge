package uwaterloo.mdc.etl.mallet;

import java.io.File;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;

public class LoadInputsIntoDocs_calllog extends LoadInputsIntoDocs {

	public enum CommDir {

		I, // INCOMING,
		M, // MISSED
		O, // OUTGOING
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	}

	public enum CommType {

		V, // VOICE,
		S, // SMS;
		D, // Data
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	}

	public enum CommContact {

		K, // IN_PHONEBOOK,
		U, // UNKNOWN};
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	}

	public enum CallDur {

		T, // Tiny <20 sec
		S, // Short <120 sec
		M, // Medium <10*60 sec
		L, // Long <35*60 sec (the five minutes is for introductions)
		H, // Hour <65*60 sec
		E, // Eternal > 65*60 sec
		Missing;
		public String toString() {
			if (this == Missing) {
				return Config.MISSING_VALUE_PLACEHOLDER;
			} else {
				return super.toString();
			}
		};
	}

	static {
		Discretize.enumsMap.put("calllog_direction",
				LoadInputsIntoDocs_calllog.CommDir.values());
		Discretize.enumsMap.put("calllog_description",
				LoadInputsIntoDocs_calllog.CommType.values());
		Discretize.enumsMap.put("calllog_in_phonebook",
				LoadInputsIntoDocs_calllog.CommContact.values());
		Discretize.enumsMap.put("calllog_duration",
				LoadInputsIntoDocs_calllog.CallDur.values());
	}

	public LoadInputsIntoDocs_calllog(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected String getTimeColumnName() {
		return "call_time";
	}

	@Override
	protected void delimiterProcedure() {

		if ("status".equals(currKey) || "number_prefix".equals(currKey)
				|| "number".equals(currKey)) {
			// We don't care about the status of sms
			// And the region being called as well
			// We also don't bother about tracking repeated numbers (TODO:
			// yet??)
		} else {

			// Timezone preceeds time only in this stupid case!

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

				} else {
					// meaningless, because it is the first record
					// System.out.println("blah.. just making sure of something!");
				}
				prevTimeColReading = currTime;
				// } else if ("tz".equals(currKey)) {
				// // We keep times in GMT.. nothing to do!
			} else {
				super.delimiterProcedure();
			}
		}
	}

	@Override
	protected Enum<?> getValueToWrite() {
		Enum<?> result = null;
		if ("direction".equals(currKey)) {
			if ("Incoming".equals(currValue)) {
				result = CommDir.I;
			} else if ("Missed call".equals(currValue)) {
				result = CommDir.M;
			} else if ("Outgoing".equals(currValue)) {
				result = CommDir.O;
			}
		} else if ("description".equals(currKey)) {
			if ("Voice call".equals(currValue)) {
				result = CommType.V;
			} else if ("Short message".equals(currValue)) {
				result = CommType.S;
			} else if ("Data call".equals(currValue)) {
				result = CommType.D;
			}
		} else if ("in_phonebook".equals(currKey)) {
			if ("1".equals(currValue)) {
				result = CommContact.K;
			} else {
				result = CommContact.U;
			}
		} else if ("duration".equals(currKey)) {
			int dur = -1;
			try {
				dur = Integer.parseInt(currValue);
			} catch (NumberFormatException ignored) {
				result = CallDur.Missing;
			}
			if (dur == 0) {
				result = CallDur.Missing;
			} else if (dur < 20) {
				result = CallDur.T;
			} else if (dur < 120) {
				result = CallDur.S;
			} else if (dur < 600) {
				result = CallDur.M;
			} else if (dur < 2100) {
				result = CallDur.L;
			} else if (dur < 3900) {
				result = CallDur.H;
			} else {
				result = CallDur.E;
			}
		}

		return result;
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		boolean result = false;
		if ("duration".equals(colName)) {
			result = true;
		}
		return result;
	}

}
