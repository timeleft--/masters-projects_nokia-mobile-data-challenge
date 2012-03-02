package uwaterloo.mdc.etl.model;

import java.io.File;
import java.util.ArrayList;

import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class UserVisitHierarchy {

	protected final File userDir;
 
	protected final ArrayList<KeyValuePair<Long, File>> visitList; 
	protected final ArrayList<Character> trust;
	
	public UserVisitHierarchy(File userDir) {
		super();
		this.userDir = userDir;
		visitList = new ArrayList<KeyValuePair<Long, File>>();
		trust  =new ArrayList<Character>();
		
		File[] visitDirArr = userDir.listFiles();

		for (File visitDir: visitDirArr) {
			if(!visitDir.isDirectory()){
				continue;
			}
			Long key = Long.parseLong(StringUtils.removeLastNChars(visitDir.getName(), 1));

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
	}
}
