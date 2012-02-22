package uwaterloo.mdc.etl.model;

import java.io.File;
import java.util.ArrayList;

import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class UserVisitHierarchy {

	protected final File userDir;
	//TODONE: write some function that does an adapted binary search 
	protected final ArrayList<KeyValuePair<Long, File>> visitList; 
	protected final ArrayList<Character> trust;
	
//	protected final Random error;
	
	public UserVisitHierarchy(File userDir) {
		super();
		this.userDir = userDir;
		visitList = new ArrayList<KeyValuePair<Long, File>>();
		trust  =new ArrayList<Character>();
		
		File[] visitDirArr = userDir.listFiles();
//Too much overhead -->	Arrays.sort(visitDirArr); //just making sure!
		for (File visitDir: visitDirArr) {
			if(!visitDir.isDirectory()){
				continue;
			}
			Long key = Long.parseLong(StringUtils.removeLastNChars(visitDir.getName(), 1));
// After a second though.. no! Let's not add uncertainty			
//			//Add gaussian error:
//			Long error;
//			if(visitDir.getName().endsWith("T")){
//				error = Config.TIME_SECONDS_IN_10MINS;
//			} else {
//				error = 
//			}
//			//The start time is conservatively kept within 10 minutes of GPS readings
//			key -= error; 

			//We want to try to find an exact match first before resorting to an error
//			// Equivalent to if (visitStartTime - startTime > Error) below
//			// but does the subtraction once
//			key -= Discretize.getStartEndTimeError(StringUtils.charAtFromEnd(visitDir.getName(), 1));
			trust.add(StringUtils.charAtFromEnd(visitDir.getName(), 1));
			visitList.add(new KeyValuePair<Long, File>(key, visitDir));
		}

	}
	
	public File getVisitDirForEndTime(long startTime){
		KeyValuePair<Long, File> pair = new TimeSearch<File>().findForTime(visitList, startTime, true, true, trust);
		if(pair!= null){
		return pair.getValue();
		} else {
			return null;
		}
//		File result = null;
//		for (KeyValuePair<Long, File> visit : visitList) {
//			long visitStartTime = visit.getKey();
//			if (visitStartTime > startTime ) {
//				if()
//				break;
//			} else {
//				result = visit.getValue();
//			}
//		}
//		return result;
	}
}
