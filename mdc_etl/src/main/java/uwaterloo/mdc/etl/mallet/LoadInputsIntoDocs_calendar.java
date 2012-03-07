package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Discretize;

public class LoadInputsIntoDocs_calendar extends LoadInputsIntoDocs {

	public LoadInputsIntoDocs_calendar(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	private boolean isRecurring;
	private Frequency titleFreq = new Frequency();

	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		colsToSkip.add("time");
		colsToSkip.add("uid");
		colsToSkip.add("status");
		// The location ID of a calendar entry will only
		// be redundant to Wifi Macs, if not misleading
		colsToSkip.add("location");
		colsToSkip.add("class");
		colsToSkip.add("last_mod");
		colsToSkip.add("title");
	}

	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}

	public enum CalendarTypeEnum {
		A, // APPOINTMENT
		E, // EVENT
		AR, // Recurring
		ER; // Recirring
	}
	
	static {
		Discretize.enumsMap.put("calendar_type", CalendarTypeEnum.values());
	}

	@Override
	protected Comparable<?> getValueToWrite() {
		// type
		CalendarTypeEnum result = null;
		if ("APPOINTMENT".equals(currValue)) {
			if (isRecurring) {
				result = CalendarTypeEnum.AR;
			} else {
				result = CalendarTypeEnum.A;
			}
		} else if ("EVENT".equals(currValue)) {
			if (isRecurring) {
				result = CalendarTypeEnum.ER;
			} else {
				result = CalendarTypeEnum.E;
			}
		}
		return result;
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

	@Override
	public String getTimeColumnName() {
		// We don't care when the calendar entry was recorded
		// But when will it actually start
		return "begin";
	}

	@Override
	protected void delimiterProcedure() {
//		// Timezone preceeds time only in this stupid case!
//		if (getColsToSkip().contains(currKey)) {
//			return;
//		}
//
//		if ("tz".equals(currKey)) {
//			// We keep times in GMT..
//			currTime = Long.parseLong(currValue);
//		} else if (currKey.equals(getTimeColumnName())) {
//			// calculateDeltaTime
//
//			currTime += Long.parseLong(currValue);
//
//			if (prevTimeColReading != null) {
//				long deltaTime = currTime - prevTimeColReading;
//				if (deltaTime != 0) {
//					// We have finished readings for one time slot.. write
//					// them
//					onTimeChanged();
//				}
//
//			} else {
//				// meaningless, because it is the first record
//				// System.out.println("blah.. just making sure of something!");
//			}
//			prevTimeColReading = currTime;
//
//		} else
			if ("title".equals(currKey)) {
			// special handling to count recurring titles
			isRecurring = (titleFreq.getCount(currValue) != 0);
			titleFreq.addValue(currValue);

		} else {
			super.delimiterProcedure();
		}
	}
}
