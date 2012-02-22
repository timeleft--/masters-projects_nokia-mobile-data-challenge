package uwaterloo.mdc.etl.model;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class UserVisitsDocsHierarchy {
	
	protected final File userDir;
	protected final ArrayList<KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>>> visitsList; 
	protected final ArrayList<Character> visitTrust;
	protected final HashMap<Long, ArrayList<Character>> microLocTrustMap;
	
	public UserVisitsDocsHierarchy(File userDir) {
		super();
		this.userDir = userDir;
		visitsList = new ArrayList<KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>>>();
		visitTrust = new ArrayList<Character>();
		microLocTrustMap = new  HashMap<Long, ArrayList<Character>>();
		
		File[] visitDirArr = userDir.listFiles();
		for (File visitDir: visitDirArr) {
			if(!visitDir.isDirectory()){
				continue;
			}
			Long key = Long.parseLong(StringUtils.removeLastNChars(visitDir.getName(), 1));
			
			KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> microLocsList = new KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>>(key, new ArrayList<KeyValuePair<Long, StringBuilder>>());
			visitsList.add(microLocsList);
			visitTrust.add(StringUtils.charAtFromEnd(visitDir.getName(), 1));
			ArrayList<Character> microLocTrust = new ArrayList<Character>();
			microLocTrustMap.put(key, microLocTrust); 
					
			File[] visitEndFiles = visitDir.listFiles();
//Unnecessary Overhead		Arrays.sort(visitEndFiles);
			for (int j = visitEndFiles.length - 1; j >= 0; --j) {
				// Descending traversal
				microLocsList.getValue().add(new KeyValuePair<Long, StringBuilder>(
						 Long.parseLong(StringUtils.removeLastNChars(visitEndFiles[j].getName(),5)), new StringBuilder()));
				microLocTrust.add(StringUtils.charAtFromEnd(visitEndFiles[j].getName(),5));
			}
		}

	}


	public StringBuilder getDocForEndTime(long endTimeInSecs){
		KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> visit = searchInVisit(endTimeInSecs, false);
		if(visit ==null){
			return null;
		}
//		Long visitStratDir = null; 
//		for (KeyValuePair<Long, LinkedList<KeyValuePair<Long, StringBuilder>>> visit: visitsList) {
//			Long visitStartTime = visit.getKey();
//			if (visitStartTime > endTimeInSecs) {
//				break;
//			} else {
//				visitStratDir = visitStartTime;
//			}
//		}

		KeyValuePair<Long, StringBuilder> microLoc = searchInMicroLocs(visit, endTimeInSecs, false);
		if(microLoc != null){
			return microLoc.getValue();
		} else {
			return null;
		}
		
//		LinkedList<KeyValuePair<Long, StringBuilder>> visitEndTimes;
//		if (visitStratDir != null) {
//			visitEndTimes = visitsList.get(visitStratDir);
//
//			for (KeyValuePair<Long, StringBuilder> visit : visitEndTimes) {
//				long locEndTime = visit.getKey();
//				if (locEndTime < endTimeInSecs) {
//					break;
//				} else {
//					result = visit.getValue();
//				}
//			}
//		}
//		
//		return result;
	}
	
	public StringBuilder getDocExact(Long visitStartTime, Long docEndTime){
		KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> visit = searchInVisit(visitStartTime, true);
		if(visit ==null){
			return null;
		}
		KeyValuePair<Long, StringBuilder> microLoc = searchInMicroLocs(visit, docEndTime, true);
		if(microLoc != null){
			return microLoc.getValue();
		} else {
			return null;
		}
//		LinkedList<KeyValuePair<Long, StringBuilder>> visit = visitsList.get(visitStartTime);
//		for(KeyValuePair<Long, StringBuilder> doc: visit){
//			if(doc.getKey().equals(docEndTime)){
//				return doc.getValue();
//			} else if(doc.getKey() < docEndTime){
//				break; // the list is sorted descending
//			}
//		}
//		return null; 
	}
	
	private KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> searchInVisit(Long startTimeInSecs, boolean exact){
		TimeSearch<ArrayList<KeyValuePair<Long, StringBuilder>>> visitSearch = new TimeSearch<ArrayList<KeyValuePair<Long, StringBuilder>>>();
		visitSearch.setExact(exact);
		KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> visit = visitSearch.findForTime(visitsList, startTimeInSecs, true, true, visitTrust);
		return visit;
	}
	
	private KeyValuePair<Long, StringBuilder> searchInMicroLocs(KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> visit, Long endTimeInSecs, boolean exact){
		TimeSearch<StringBuilder> microLocSearch = new TimeSearch<StringBuilder>();
		microLocSearch.setExact(exact);
		KeyValuePair<Long, StringBuilder> microLoc = microLocSearch.findForTime(visit.getValue(), endTimeInSecs, false, false, microLocTrustMap.get(visit.getKey()));
		return microLoc;
	}
}
