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
