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
			ArrayList<Character> microLocTrust = new ArrayList<Character>();
			
			File[] visitEndFiles = visitDir.listFiles();

			for (int j = visitEndFiles.length - 1; j >= 0; --j) {
				// Descending traversal
				microLocsList.getValue().add(new KeyValuePair<Long, StringBuilder>(
						 Long.parseLong(StringUtils.removeLastNChars(visitEndFiles[j].getName(),5)), new StringBuilder()));
				microLocTrust.add(StringUtils.charAtFromEnd(visitEndFiles[j].getName(),5));
			}
			if(microLocsList.getValue().size() == 0){
				// No files inside any more.. just a sanity check! 
				// Because the data is insane: user 039 has visits that start and end in different timezones
				// Actually too many to be those that started just before the day light saving time
				continue; 
			}
			visitsList.add(microLocsList);
			visitTrust.add(StringUtils.charAtFromEnd(visitDir.getName(), 1));
			
			microLocTrustMap.put(key, microLocTrust); 

		}

	}


	public StringBuilder getDocForEndTime(long endTimeInSecs){
		KeyValuePair<Long, ArrayList<KeyValuePair<Long, StringBuilder>>> visit = searchInVisit(endTimeInSecs, false);
		if(visit ==null){
			return null;
		}

		KeyValuePair<Long, StringBuilder> microLoc = searchInMicroLocs(visit, endTimeInSecs, false);
		if(microLoc != null){
			return microLoc.getValue();
		} else {
			return null;
		}
		
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
