package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.KeyValuePair;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.SVMLightSaver;

public class SVMLighSaver {
	private static final String ARFF_DIR = "C:\\mdc-datasets\\weka\\segmented_user";
	private static final String OUT_DIR = "C:\\mdc-datasets\\svmlight\\output";
	private static final String IN_DIR = "C:\\mdc-datasets\\svmlight\\input";

	static void saveFold(File positiveClassDir, int foldStart)
			throws IOException {

		ArrayList<File> dataFiles = new ArrayList<File>();
		dataFiles.addAll(Arrays.asList(positiveClassDir
				.listFiles(new FilenameFilter() {

					@Override
					public boolean accept(File dir, String name) {

						return name.endsWith(".arff");
					}
				})));

		Instances validationSet = new Instances(Channels.newReader(FileUtils
				.openInputStream(dataFiles.remove(foldStart)).getChannel(),
				Config.OUT_CHARSET));
		validationSet.setClassIndex(validationSet.numAttributes() - 1);

		
		SVMLightSaver svmlightSave = new SVMLightSaver();
		svmlightSave.setInstances(validationSet);
		svmlightSave.setDestination(FileUtils.openOutputStream(FileUtils.getFile(IN_DIR, positiveClassDir.getName(), "v" + foldStart, "validate.csv")));
		svmlightSave.writeBatch();
		validationSet = null;
		svmlightSave = null;

		Instances trainingSet = loadAugmentedInstances(dataFiles);
		
		svmlightSave = new SVMLightSaver();
		svmlightSave.setInstances(trainingSet);
		svmlightSave.setDestination(FileUtils.openOutputStream(FileUtils.getFile(IN_DIR, positiveClassDir.getName(), "v" + foldStart, "input.csv")));
		svmlightSave.writeBatch();
		trainingSet = null;

		File outDir = FileUtils.getFile(OUT_DIR, positiveClassDir.getName(), "v" + foldStart);
		for(int t=0;t<4;++t){
			FileUtils.openOutputStream(FileUtils.getFile(outDir,"t"+t+"_trans.txt")).close();
			FileUtils.openOutputStream(FileUtils.getFile(outDir,"t"+t+"_alpha.txt")).close();
			FileUtils.openOutputStream(FileUtils.getFile(outDir,"t"+t+"_model.txt")).close();
			FileUtils.openOutputStream(FileUtils.getFile(outDir,"t"+t+"_predictions.txt")).close();
		}
	}

	static Instances loadAugmentedInstances(ArrayList<File> dataFiles)
			throws IOException {
		Instances trainingSet = null;

		int userIx = 0;
		for (File userData : dataFiles) {
			if (userIx == Config.NUM_USERS_TO_PROCESS) {
				break;
			}

			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(userData);

			// load structure
			Instances dataStruct = dataLoader.getStructure();
			dataStruct.setClassIndex(dataStruct.numAttributes() - 1);

			if (userIx == 0) {
				trainingSet = new Instances(dataStruct);
			}

			// load data
			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {
				trainingSet.add(dataInst);
			}

			++userIx;
		}
		return trainingSet;
	}

	public static void main(String[] args) throws IOException {
		for(int foldStart: Arrays.asList(8, 24, 5, 9, 11, 19, 4, 6, 64, 46, 78, 68, 66, 58, 12, 57, 54, 74, 30, 1)){
//				15, 8, 64, 63, 10, 53, 5, 50, 54, 11, 60, 59, 30, 49, 33, 2, 35, 32, 43, 52)){
		for(File arffDir: FileUtils.getFile(ARFF_DIR).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File arg0, String arg1) {
				return arg1.startsWith("c");
			}
		})){
			saveFold(arffDir, foldStart);
		}
		}
	}
}
