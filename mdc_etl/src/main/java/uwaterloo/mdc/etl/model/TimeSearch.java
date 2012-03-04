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

		// int i = start;
		if (exactRes != null) {
			return exactRes;
		} else if (exactRes == null && !exact) {
			// Too asvanced for me.. I just can't do it :'(
			// int miusOrPlus = (startOrEnd ? -1 : +1);
			//
			// if (i == timeList.size()) {
			// i = timeList.size() - 1;
			// }
			//
			// if (timeList.size() == 1) {
			// if ((ascending && timeList.get(0).getKey() <= time)
			// || (!ascending && timeList.get(0).getKey() >= time)) {
			// return timeList.get(0);
			// }
			// }
			//
			// Integer lastComparison = null;
			// KeyValuePair<Long, V> prev = null;
			// while (i >= 0 && i < timeList.size()) {
			// KeyValuePair<Long, V> current = timeList.get(i);
			//
			// // if (Math.abs(result.getKey() - time) <= Discretize
			// // .getStartEndTimeError(trust.get(i))) {
			// // return result;
			// // }
			// if (prev != null
			// && (Math.abs(current.getKey() - time) > Math.abs(prev
			// .getKey() - time))) {
			// // we are moving away.. result was the best
			// return prev;
			// }
			//
			// int comparison = current.getKey().compareTo(time);
			// if (lastComparison != null && comparison != lastComparison) {
			// // We have moved past the saddle point
			// if ((ascending && prev.getKey() <= time)
			// || (!ascending && prev.getKey() >= time)) {
			// return prev;
			// } else if ((ascending && current.getKey() <= time)
			// || (!ascending && current.getKey() >= time)) {
			// return current;
			// } else {
			// // return null;
			// break; // just to try approximation
			// }
			// }
			// prev = current;
			// lastComparison = comparison;
			//
			// // If comparison is -1, then the current elt is less than
			// // startTime
			// // so we need to move up the ascending list, until the elt is
			// // greater.
			// // If comparison is +1, we do exactly the opposite.
			// // And the direction inverses all of this in case of descending.
			// i += miusOrPlus * comparison * direction;
			// }
			// Long error;
			// if (!ascending) {
			// // Does it really matter? Because we then have to handle size =
			// // 1 &&
			// // i == -1) {
			// error = time - timeList.get(0).getKey();
			// if (0 < error
			// && error <= Discretize.getStartEndTimeError(trust
			// .get(0))) {
			// return timeList.get(0);
			// }
			// }
			// if (ascending) { // && i == timeList.size()) {
			// error = timeList.get(timeList.size() - 1).getKey() - time;
			// if (0 < error
			// && error <= Discretize.getStartEndTimeError(trust
			// .get(trust.size() - 1))) {
			// return timeList.get(timeList.size() - 1);
			// }
			// }
		}
		return null;
	}

	public void setExact(boolean exact) {
		this.exact = exact;
	}

}
