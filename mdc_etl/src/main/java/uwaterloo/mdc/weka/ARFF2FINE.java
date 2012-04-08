package uwaterloo.mdc.weka;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Enumeration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

public class ARFF2FINE {
	private static final int NUM_USERS_TO_PROCESS = 80;
	static String inPath = "/Users/yia/Dropbox/nokia-mdc/segmented_user/ALL/";
	static String outPath = FilenameUtils.concat(inPath, "matlab");

	public static void main(String[] args) throws IOException {
		Instances insts = null;
		int fileIx = 0;
		for (File inFile : FileUtils.listFiles(FileUtils.getFile(inPath),
				new String[] { "arff" }, false)) {
			if (fileIx == NUM_USERS_TO_PROCESS) {
				break;
			}
			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(inFile);

			Instances dataStructure = dataLoader.getStructure();
			dataStructure.setClassIndex(dataStructure.numAttributes() - 1);

			if (fileIx == 0) {
				insts = new Instances(dataStructure);
			}

			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStructure)) != null) {
				insts.add(dataInst);
			}
			++fileIx;
		}

		// supervised and unsupervised
		for(int s=0; s<=1; ++s){
			
		
		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath, s+"mdc.csv")).getChannel(),
				"US-ASCII");

		try {
			// skip ID and label
			for (int a = 1; a < insts.numAttributes()-s; ++a) {
				Enumeration instEnum = insts.enumerateInstances();
				while (instEnum.hasMoreElements()) {
					Instance inst = (Instance) instEnum.nextElement();
					wr.append(Double.toString(inst.value(a))).append('\t');
				}
				wr.append('\n');
			}
		} finally {
			wr.flush();
			wr.close();
		}
		}
	}
}
