package uwaterloo.mdc.weka;

import java.io.File;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;

import weka.core.Attribute;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;
import weka.core.converters.ArffSaver;

public class PCARemovedConsensus implements Callable<Void> {
	private static final int NUM_USERS_TO_PROCESS = 80;
	private static final String APPDICT_PROPS_PATH = "C:\\mdc-datasets\\app-uid_name.properties";
	private static Properties appDictionary;

	String inPath = "C:\\mdc-datasets\\weka\\segmented_user\\transformed";
	String structFile = "C:\\mdc-datasets\\weka\\segmented_user\\ALL\\001.arff";
	String CLASSIFIER_TO_HONOUR = "weka.classifiers.trees.RandomForest"; //"weka.classifiers.trees.J48";
	String FEAT_SELECTOR_TO_HONOUR = "weka.attributeSelection.PrincipalComponents";
	String outPath = "C:\\mdc-datasets\\weka\\filters";

	public static void main(String[] args) throws Exception {
		appDictionary = new Properties();
		appDictionary.load(FileUtils.openInputStream(FileUtils
				.getFile(APPDICT_PROPS_PATH)));
		new PCARemovedConsensus().call();
	}

	public Void call() throws Exception {
		StringBuilder topRemoved = new StringBuilder();

		TreeMap<Integer, Integer> removedCount = new TreeMap<Integer, Integer>(
				new Comparator<Integer>() {

					public int compare(Integer o1, Integer o2) {
						return -o1.compareTo(o2);
					}
				});
		Reader structRd = Channels.newReader(
				FileUtils.openInputStream(FileUtils.getFile(structFile))
						.getChannel(), "US-ASCII");
		ArffReader arffRd = new ArffReader(structRd);
		Instances struct = arffRd.getStructure();
		Instances refinedStruct = arffRd.getStructure();
		structRd.close();
		arffRd = null;

		int fileIx = 0;
		for (File inFile : FileUtils.listFiles(FileUtils.getFile(inPath,
				CLASSIFIER_TO_HONOUR, FEAT_SELECTOR_TO_HONOUR, "ALL"),
				new String[] { "arff" }, false)) {
			if (fileIx == NUM_USERS_TO_PROCESS) {
				break;
			}
			LinkedList<Integer> removedAttrs = new LinkedList<Integer>();
			Reader rd = Channels.newReader(FileUtils.openInputStream(inFile)
					.getChannel(), "US-ASCII");
			try {
				boolean interesting = false;
				boolean interval = false;
				int chInt;
				StringBuffer buff = new StringBuffer();
				while ((chInt = rd.read()) != -1) {
					char ch = (char) chInt;
					if (ch == '\n') {
						break;
					} // else
					if (ch != '-' && ch != ',') {
						buff.append(ch);
					} else if (!interesting) {
						if (buff.indexOf("Remove") != -1) {
							interesting = true;
							interval = false;
							// skip the R
							rd.read();

							buff.setLength(0);
						}
					} else if (interesting) {
						// if(buff.indexOf("weka"))
						int attrIx = Integer.parseInt(buff.toString());
						if (interval) {
							for (int a = removedAttrs.peekLast() + 1; a <= attrIx; ++a) {
								removedAttrs.addLast(a);
							}
						} else {
							removedAttrs.addLast(attrIx);
						}
						interval = (ch == '-');

						buff.setLength(0);
					}
				}
			} finally {
				rd.close();
			}

			for (Integer attrIx : removedAttrs) {

				Integer cnt = removedCount.get(attrIx);
				if (cnt == null) {
					cnt = 0;
				}
				removedCount.put(attrIx, cnt + 1);
			}

			++fileIx;
		}

		// for(String attrName: removedCount.keySet()){
		// topRemoved.put(removedCount.get(attrName), attrName);
		// }
		//
		// FileUtils.writeStringToFile(FileUtils.getFile(outPath),
		// topRemoved.toString());

		Writer wrRmv = Channels
				.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(outPath,CLASSIFIER_TO_HONOUR,FEAT_SELECTOR_TO_HONOUR, "removed.csv"))
								.getChannel(), "US-ASCII");
		Writer wrFilter = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath,CLASSIFIER_TO_HONOUR,FEAT_SELECTOR_TO_HONOUR, "remove-filter.txt"))
						.getChannel(), "US-ASCII");

		try {
			wrRmv.append("Attribute\tRemovedCount\n");
			for (Integer attrIx : removedCount.keySet()) {
				if (attrIx == struct.numAttributes()) {
					// FIXME: make sure that the label is really removed in PCA
					// not that the indeces are shifted by 1 for some other
					// reason
					continue;
				}
				Integer cnt = removedCount.get(attrIx);
				if (cnt > NUM_USERS_TO_PROCESS / 2) {
					refinedStruct.deleteAttributeAt(attrIx - 1);

					String attrName = struct.attribute(attrIx - 1).name();
					if (attrName.startsWith("[")) {
						attrName = appDictionary.getProperty(attrName);
					}

					wrRmv.append(attrName).append('\t')
							.append(Integer.toString(cnt)).append('\n');
					wrFilter.append(attrIx + ",");
				}
			}
		} finally {
			wrRmv.flush();
			wrRmv.close();
			wrFilter.flush();
			wrFilter.close();
		}

		ArffSaver arfWr = new ArffSaver();
		OutputStream refinedArff = FileUtils.openOutputStream(FileUtils
				.getFile(outPath, CLASSIFIER_TO_HONOUR,FEAT_SELECTOR_TO_HONOUR,"refined.arff"));

		try {

			arfWr.setDestination(refinedArff);
			arfWr.setInstances(refinedStruct);
			arfWr.writeBatch();
		} finally {

			refinedArff.flush();
			refinedArff.close();
		}
		Writer wrRmg = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath,CLASSIFIER_TO_HONOUR,FEAT_SELECTOR_TO_HONOUR, "remaining-apps.csv"))
						.getChannel(), "US-ASCII");
		try {
			Enumeration attrEnum = refinedStruct.enumerateAttributes();
			while (attrEnum.hasMoreElements()) {
				Attribute attr = (Attribute) attrEnum.nextElement();
				String attrName = attr.name();
				if (attrName.startsWith("[")) {
					attrName = appDictionary.getProperty(attrName);
					wrRmg.append(attrName).append('\n');
				}
			}
		} finally {
			wrRmg.flush();
			wrRmg.close();
		}
		return null;
	}
}
