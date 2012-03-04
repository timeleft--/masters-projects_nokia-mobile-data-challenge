package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import cc.mallet.types.Instance;

public class VisitsIterator implements Iterator<Instance> {

	final Pattern tabSplit = Pattern.compile("\\t");

	final File dataDirParent;
	final HashMap<File, Integer> dirIndexMap;

	File currDir;
	File[] currFiles;
	
	Instance nextInst;

	public VisitsIterator(File dataDir) {
		
		this.dataDirParent = dataDir.getParentFile();
		dirIndexMap = new HashMap<File, Integer>();

		goIntoDir(dataDir);
		
		nextInst = nextInternal();
	}

	void goIntoDir(File dir) {
		currDir = dir;
		dirIndexMap.put(currDir, -1);
		currFiles = currDir.listFiles();
	}

	@Override
	public Instance next() {
		Instance result = nextInst;
		nextInst = nextInternal();
		return result;
	} 
	
	public Instance nextInternal(){
		int i = dirIndexMap.get(currDir);
		++i;

		if (i < currFiles.length) {
			dirIndexMap.put(currDir, i);
			if (currFiles[i].isDirectory()) {
				goIntoDir(currFiles[i]);
				return nextInternal();
			} else {
				try {
					String instStr = FileUtils.readFileToString(currFiles[i],
							Config.OUT_CHARSET);
					String[] instFields = tabSplit.split(instStr);
					String target = Config.placeLabels
							.getProperty(instFields[1]);
					if (target != null) {
						String name = instFields[0] + "@" + instFields[1];
						return new Instance(instFields[2], target, name,
								currFiles[i].getAbsolutePath());
					} else {
						return nextInternal();
					}
				} catch (IOException ignore) {
					return nextInternal();
				}
			}
		} else {
			if (currDir.getParentFile().equals(dataDirParent)) {
				return null;
			} else {
				dirIndexMap.remove(currDir);
				currDir = currDir.getParentFile();
				currFiles = currDir.listFiles();
				return nextInternal();
			}
		}

	}

	@Override
	public boolean hasNext() {
//		return !(dirIndexMap.get(currDir) == (currFiles.length - 1) && currDir
//				.getParentFile().equals(dataDirParent));
		return nextInst != null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
