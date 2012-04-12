package uwaterloo.mdc.weka;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import weka.core.Attribute;
import weka.core.Capabilities;
import weka.core.Capabilities.Capability;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.SVMLightSaver;

public class SVMLighSaver {
	
	static class CapableSaver extends weka.core.converters.SVMLightSaver {
		public Capabilities getCapabilities() {
		    Capabilities result = super.getCapabilities();
		    
		   
		    result.enable(Capability.NOMINAL_CLASS);
		    if(Config.LOAD_COUNT_TRANSDUCTIVE_ZERO){
		    result.enable(Capability.MISSING_CLASS_VALUES);
		    }
		    
		    return result;
		  }
		
		@Override
		protected String instanceToSvmlight(Instance inst) {
			Attribute clsAttr = inst.classAttribute();
			boolean classWasMissing = inst.classIsMissing() && Config.LOAD_COUNT_TRANSDUCTIVE_ZERO;
			if(classWasMissing){
				inst.setClassValue(clsAttr.value(0));
			}
			String result = super.instanceToSvmlight(inst);
			result = result.substring(result.indexOf(' '));
			String label = (clsAttr.value((int)inst.classValue()));
			if(classWasMissing){
				label = "0";
			} else if("0".equals(label)
					// replace only in case of multinomial..
					&& inst.classAttribute().numValues() > 2){
				label = "11";
			}
			result = label + result;
			return result;
		}
	}
	private static final String ARFF_DIR = "C:\\mdc-datasets\\weka\\segmented_user_full_no-weight_binary";
	private static final String OUT_DIR = "C:\\mdc-datasets\\svmlight\\output";
	private static final String IN_DIR = "C:\\mdc-datasets\\svmlight\\input";
	private static final boolean APPLY_FILTER = true;
	private static final String FILTER_TEXT = "1,2,3,4,6,7,9,10,11,13,14,15,17,16,19,18,21,20,23,22,25,27,26,29,28,30,34,35,33,38,39,37,42,43,40,41,46,47,44,45,50,49,48,55,54,53,52,59,58,57,56,63,62,68,69,70,71,66,67,76,77,78,72,73,74,75,85,84,86,81,80,83,82,93,92,95,94,89,91,90,102,101,96,97,106,105,119,118,117,116,115,127,126,125,124,123,122,121,120,137,136,139,138,141,140,129,128,155,156,157,158,144,145,146,147,148,149,171,170,169,174,173,172,160,165,164,189,205,204,207,206,199,222,223,217,208,209,239,234,228";
	private static int[] filteredAttrs; 
	
	static class FoldSaver implements Callable<Void>{
		private File positiveClassDir;
		private int foldStart;
		FoldSaver(File positiveClassDir, int foldStart){
			this.positiveClassDir =positiveClassDir;
			this.foldStart = foldStart;
		}
	public Void call() throws Exception {

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
		validationSet = applyFilter(validationSet);

		SVMLightSaver svmlightSave = new CapableSaver();
		svmlightSave.setInstances(validationSet);
		svmlightSave.setDestination(FileUtils.openOutputStream(FileUtils
				.getFile(IN_DIR, positiveClassDir.getName(), "v" + foldStart,
						"validate.csv")));
		svmlightSave.writeBatch();
		validationSet = null;
		svmlightSave = null;

		Instances trainingSet = loadAugmentedInstances(dataFiles);

		trainingSet = applyFilter(trainingSet);

		svmlightSave = new CapableSaver();
		svmlightSave.setInstances(trainingSet);
		svmlightSave.setDestination(FileUtils.openOutputStream(FileUtils
				.getFile(IN_DIR, positiveClassDir.getName(), "v" + foldStart,
						"input.csv")));
		svmlightSave.writeBatch();
		trainingSet = null;

		File outDir = FileUtils.getFile(OUT_DIR, positiveClassDir.getName(),
				"v" + foldStart);
		for (int t = 0; t < 4; ++t) {
			FileUtils.openOutputStream(
					FileUtils.getFile(outDir, "t" + t + "_trans.txt")).close();
			FileUtils.openOutputStream(
					FileUtils.getFile(outDir, "t" + t + "_alpha.txt")).close();
			FileUtils.openOutputStream(
					FileUtils.getFile(outDir, "t" + t + "_model.txt")).close();
			FileUtils.openOutputStream(
					FileUtils.getFile(outDir, "t" + t + "_predictions.txt"))
					.close();
		}
		return null;
	}

	}
	private static Instances applyFilter(Instances trainingSet) {
		if(APPLY_FILTER){
//			Remove rem = new Remove();
//			rem.setInputFormat(trainingSet);
//			rem.setAttributeIndices(FILTER_TEXT);
//			
//			int numAttrsBefore = trainingSet.numAttributes();
//			trainingSet.setClassIndex(-1);
//			trainingSet = Filter.useFilter(trainingSet, rem);
//			trainingSet.setClassIndex(trainingSet.numAttributes()-1);
//			
//			if(numAttrsBefore)// will alwyas happen
			for(int i=filteredAttrs.length-1; i>=0; --i){
				trainingSet.deleteAttributeAt(filteredAttrs[i]);
			}
			trainingSet.setClassIndex(trainingSet.numAttributes()-1);
		}
		return trainingSet;
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

	public static void main(String[] args) throws Exception {
		
//		LoadCountsAsAttributes.main(args);
		
		String[] attrs = FILTER_TEXT.split("\\,");
		filteredAttrs = new int[attrs.length];
		for(int i=0; i<attrs.length; ++i){
			filteredAttrs[i] = Integer.parseInt(attrs[i]);
		}
		Arrays.sort(filteredAttrs);
		
		ExecutorService exec = Executors.newFixedThreadPool(Config.NUM_THREADS);
		List<Future<Void>> futureList = new LinkedList<>();
		
		for (int foldStart : Arrays.asList(8, 24, 5, 9, 11, 19, 4, 6, 64, 46,
				78, 68, 66, 58, 12, 57, 54, 74, 30, 1)) {
			// 15, 8, 64, 63, 10, 53, 5, 50, 54, 11, 60, 59, 30, 49, 33, 2, 35,
			// 32, 43, 52)){
			for (File arffDir : FileUtils.getFile(ARFF_DIR).listFiles(
					new FileFilter() {
						@Override
						public boolean accept(File arg0) {
							return arg0.isDirectory();
						}
					})) {
				futureList.add(exec.submit(new FoldSaver(arffDir, foldStart)));
			}
		}
		
		for(Future<Void> f: futureList){
			f.get();
		}
		
		exec.shutdown();
		while(!exec.isTerminated()){
			Thread.sleep(5000);
		}
	}
}
