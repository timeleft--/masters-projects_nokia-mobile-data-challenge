package uwaterloo.mdc.etl.model;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentSkipListMap;

import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class UserVisitsDocsHierarchy {
	
	protected final File userDir;
	protected final ConcurrentSkipListMap<Long, LinkedList<KeyValuePair<Long, StringBuilder>>> userHierarchy; 
	
	public UserVisitsDocsHierarchy(File userDir) {
		super();
		this.userDir = userDir;
		
		userHierarchy = new ConcurrentSkipListMap<Long, LinkedList<KeyValuePair<Long, StringBuilder>>>();
		File[] visitDirArr = userDir.listFiles();
		for (File visitDir: visitDirArr) {
			if(!visitDir.isDirectory()){
				continue;
			}
			Long key = Long.parseLong(StringUtils.removeLastNChars(visitDir.getName(), 1));
			
			LinkedList<KeyValuePair<Long, StringBuilder>> visitHeirarchy = new LinkedList<KeyValuePair<Long, StringBuilder>>();
			userHierarchy.put(key, visitHeirarchy);
			
			File[] visitEndFiles = visitDir.listFiles();
			Arrays.sort(visitEndFiles);
			for (int j = visitEndFiles.length - 1; j >= 0; --j) {
				// Descending traversal
				visitHeirarchy.add(new KeyValuePair<Long, StringBuilder>(
						 Long.parseLong(StringUtils.removeLastNChars(visitEndFiles[j].getName(),5)), new StringBuilder()));
			}
		}

	}

	public StringBuilder getDocForEndTime(long endTimeInSecs){
		StringBuilder result = null;
		
		Long visitStratDir = null; 
		for (Long visitStartTime: userHierarchy.keySet()) {
			if (visitStartTime > endTimeInSecs) {
				break;
			} else {
				visitStratDir = visitStartTime;
			}
		}

		LinkedList<KeyValuePair<Long, StringBuilder>> visitEndTimes;
		if (visitStratDir != null) {
			visitEndTimes = userHierarchy.get(visitStratDir);

			for (KeyValuePair<Long, StringBuilder> visit : visitEndTimes) {
				long locEndTime = visit.getKey();
				if (locEndTime < endTimeInSecs) {
					break;
				} else {
					result = visit.getValue();
				}
			}
		}
		
		return result;
	}
	
	public StringBuilder getDocDirect(Long visitStartTime, Long docEndTime){
		LinkedList<KeyValuePair<Long, StringBuilder>> visit = userHierarchy.get(visitStartTime);
		for(KeyValuePair<Long, StringBuilder> doc: visit){
			if(doc.getKey().equals(docEndTime)){
				return doc.getValue();
			} else if(doc.getKey() < docEndTime){
				break; // the list is sorted descending
			}
		}
		return null; 
	}
}
