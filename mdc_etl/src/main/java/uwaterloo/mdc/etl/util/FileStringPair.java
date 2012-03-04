package uwaterloo.mdc.etl.util;

import java.io.File;

public class FileStringPair extends KeyValuePair<File,String>{

	public FileStringPair() {
		super(null, "");
	}
	public FileStringPair(File key, String value) {
		super(key, value);
		
	}
	
}
