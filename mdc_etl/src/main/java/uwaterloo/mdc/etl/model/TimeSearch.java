package uwaterloo.mdc.etl.model;

import java.util.ArrayList;

import uwaterloo.mdc.etl.util.KeyValuePair;

public class TimeSearch<V> {
	int direction;
	int start;
	int end;
	int prevStart;
	int prevEnd;
	boolean exact;

	private KeyValuePair<Long, V> findExact(
			ArrayList<KeyValuePair<Long, V>> timeList, Long startTime,
			boolean ascending) {
		prevStart = start;
		prevEnd = end;
		do {
			// end-start/2 will be zero in case of a difference of +1 or -1
			int i = start + ((end - start) / 2);

			int comparison = timeList.get(i).getKey().compareTo(startTime);

			comparison *= direction;

			if (comparison == 0) {
				return timeList.get(i);
			} else if (comparison < 0) {
				prevStart = start;
				start = i + 1;
			} else {
				prevEnd = end;
				end = i - 1;
			}

		} while (start <= end);

		return null;
	}

	public KeyValuePair<Long, V> findForTime(
			ArrayList<KeyValuePair<Long, V>> timeList, Long time,
			// always looking for start when ascending and end when descending
			// boolean startOrEnd,
			boolean ascending, ArrayList<Character> trust) {
		direction = (ascending ? 1 : -1);

		start = 0;
		end = timeList.size() - 1;

		KeyValuePair<Long, V> exactRes = findExact(timeList, time, ascending);

		return exactRes;

	}

	public void setExact(boolean exact) {
		this.exact = exact;
	}

}
