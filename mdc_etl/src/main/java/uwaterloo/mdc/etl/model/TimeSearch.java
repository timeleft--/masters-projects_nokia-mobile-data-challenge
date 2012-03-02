package uwaterloo.mdc.etl.model;

import java.util.ArrayList;

import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.util.KeyValuePair;

public class TimeSearch<V> {
	private int direction;
	private int start;
	private int end;
	private boolean exact;

	private KeyValuePair<Long, V> findExact(ArrayList<KeyValuePair<Long, V>> timeList, Long startTime,
			boolean ascending){
		do{
			// end-start/2 will be zero in case of a difference of +1 or -1
			int i = start + ((end-start)/2);
			int comparison = timeList.get(i).getKey().compareTo(startTime);
			
			comparison *= direction;
			
			if(comparison == 0){
				return timeList.get(i);
			} else if(comparison < 0) {
				start = i+1;
			} else {
				end = i-1;
			}
			
		} while(start<end);
		
		return null;
	}
	
	public KeyValuePair<Long, V> findForTime(
			ArrayList<KeyValuePair<Long, V>> timeList, Long time, boolean startOrEnd,
			boolean ascending, ArrayList<Character> trust) {
		direction = (ascending?1:-1);
		
		start = 0;
		end = timeList.size()-1;
		
		KeyValuePair<Long, V> result = findExact(timeList, time, ascending);
		
		if(result == null && !exact){
		
			int miusOrPlus = (startOrEnd? -1: +1); 
			
			int i = start;
			if(i== timeList.size()){
				i= timeList.size();
			}
			
			Integer lastComparison = null;
			while(i>=0 && i<timeList.size()){
				result = timeList.get(i);
				
				if(Math.abs(result.getKey() - time) <= Discretize.getStartEndTimeError(trust.get(i))) { 
					return result;
				}
				
				int comparison = result.getKey().compareTo(time);
				if(lastComparison != null && comparison != lastComparison){
					// We have moved past the saddle point
					return null;
				} 
				lastComparison = comparison;
				
				// If comparison is -1, then the current elt is less than startTime
				// so we need to move up the ascending list, until the elt is greater.
				// If comparison is +1, we do exactly the opposite.
				i += miusOrPlus * comparison * direction;
			}
		}
		
		return result;
	}

	public void setExact(boolean exact) {
		this.exact = exact;
	}
	
	
}
