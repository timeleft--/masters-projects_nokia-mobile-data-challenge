package uwaterloo.mdc.etl.model;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.util.FileStringPair;
import uwaterloo.mdc.etl.util.KeyValuePair;

public class UserVisitHierarchy extends UserVisitsDocsHierarchy<FileStringPair> {

	public UserVisitHierarchy(File userDir) throws Exception {
		super(userDir, FileStringPair.class.getConstructor());
	}

	// protected final File userDir;
	//
	// protected final ArrayList<KeyValuePair<Long, File>> visitList;
	// protected final ArrayList<Character> trust;
	//
	// public UserVisitHierarchy(File userDir) {
	// super();
	// this.userDir = userDir;
	// visitList = new ArrayList<KeyValuePair<Long, File>>();
	// trust =new ArrayList<Character>();
	//
	// File[] visitDirArr = userDir.listFiles();
	//
	// for (File visitDir: visitDirArr) {
	// if(!visitDir.isDirectory()){
	// continue;
	// }
	// Long key =
	// Long.parseLong(StringUtils.removeLastNChars(visitDir.getName(), 1));
	//
	// trust.add(StringUtils.charAtFromEnd(visitDir.getName(), 1));
	// visitList.add(new KeyValuePair<Long, File>(key, visitDir));
	// }
	//
	// }
	//
	// public File getVisitDirForEndTime(long startTime){
	// KeyValuePair<Long, File> pair = new
	// TimeSearch<File>().findForTime(visitList, startTime, /*true,*/ true,
	// trust);
	// if(pair!= null){
	// return pair.getValue();
	// } else {
	// return null;
	// }
	// }
	@Override
	protected FileStringPair getValueObj(Constructor<FileStringPair> vConstr,
			File visitEndFile) throws Exception {
		String placeid = FileUtils.readFileToString(visitEndFile);
		return new FileStringPair(visitEndFile, placeid);
	}
	
	public void addMicroLoc(Visit<FileStringPair> visit, Long endTime, Character mlTrust, FileStringPair value){
		ArrayList<KeyValuePair<Long, FileStringPair>> microLocsList = visit.getValue();
		ArrayList<Character> microLocTrust = microLocTrustMap.get(visit.getKey());
		
		// Descending traversal
		int i;
		for(i=0; i<microLocsList.size(); ++i){
			if(microLocsList.get(i).getKey() < endTime){
				break;
			}
		}
		microLocsList.add(i,
				new KeyValuePair<Long, FileStringPair>(endTime,
								value));
		microLocTrust.add(i, mlTrust);
	}
	
	public Iterator<KeyValuePair<Long, ArrayList<KeyValuePair<Long, FileStringPair>>>> getVisitListIterator(){
		return this.visitsList.iterator();
	}
}
