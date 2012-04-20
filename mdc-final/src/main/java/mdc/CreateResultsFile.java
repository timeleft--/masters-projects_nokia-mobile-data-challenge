package mdc;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

public class CreateResultsFile {

	static class NBResFilter implements FilenameFilter{
		static String FILENAME_SUFFIX = "place-distribs-afterEM.csv";
		double s;
		int b;
		
		public NBResFilter(double s, int b) {
			super();
			this.s = s;
			this.b = b;
		}

		public boolean accept(File dir, String name) {
			boolean result = name.endsWith(FILENAME_SUFFIX)
//			 && name.contains("_l3")
			 && name.contains("n" + s)
			 && name.contains("b" + b);							 
			
			return result;
		}
		
	}
	
	private static String outPath = "D:\\mdc-datasets\\submitted";
	private static String inPath = "D:\\mdc-datasets\\weka\\final-out_diff-priors_big-penalized\\weka.classifiers.trees.RandomForest\\info-gain";
//			"D:\\mdc-datasets\\weka\\final-out_likely\\weka.classifiers.trees.RandomForest\\info-gain";

	static Random rand = new Random(System.currentTimeMillis());
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		for (double s : new double[] {1.0, 1.15}) {
			for (int b : new int[] {0, 1, 2}) {
				Writer wr = Channels.newWriter(
						FileUtils.openOutputStream(
								FileUtils.getFile(outPath , "task1_yaboulna_s"
										+ s + "_b" + b + ".csv")).getChannel(),
						"US-ASCII");
				try{
					wr.append("userid\tplace_id\tlabel\n");
					File[] nbFiles = FileUtils.getFile(inPath).listFiles(new NBResFilter(s, b));
					for(File resFile: nbFiles){
						CSVLoader csvLoad = new CSVLoader();
						csvLoad.setSource(resFile);
						// Instances struct = csvLoad.getStructure();
						Instances data = csvLoad.getDataSet();
						for(int i=0; i<data.numInstances(); ++i){
							Instance inst = data.instance(i);
							double label = inst.value(14);
//							if(inst.value(14) == 0.0){
//								continue;
//							}
							if(label == 0){
								label = 4 + rand.nextInt(7);
							}
							
							wr.append(Long.toString(Math.round(inst.value(0)))).append("\t")
							 .append(Long.toString(Math.round(inst.value(1)))).append("\t")
							 .append(Long.toString(Math.round(label))).append("\n");
						}
					}
					
				}finally{
					wr.flush();
					wr.close();
				}
			}
		}
	}
}
