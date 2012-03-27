package uwaterloo.mdc.etl;

import java.io.File;
import java.io.FileFilter;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import uwaterloo.mdc.etl.mallet.ImportIntoMallet.MalletImportedFiles;
import uwaterloo.mdc.etl.mallet.LoadInputsIntoDocs;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;

public class DiscritizeTest {

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated\\001";
	private String outPath = "C:\\mdc-datasets\\mallet";
	CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder> loadDataFactory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Object>>, StringBuilder>();
	
	@Test
	public void testShortKey() throws Exception {
		File userDir = FileUtils.getFile(dataRoot);
		FileFilter dataFileFilter = new MalletImportedFiles();
		for(File dataFile: userDir.listFiles(dataFileFilter)){
			LoadInputsIntoDocs loadDataOp = (LoadInputsIntoDocs) loadDataFactory
					.createOperation(LoadInputsIntoDocs.class,
							this, dataFile, "");
			HashSet<String> skippedCols = loadDataOp.getColsToSkip();
			
			skippedCols.add(Config.USERID_COLNAME);
			skippedCols.add(loadDataOp.getTimeColumnName());
			skippedCols.add("tz");
			
			for(String colname: loadDataOp.getKeyList()){
				if(!skippedCols.contains(colname)){
					Discretize.getShortKey(dataFile, colname);
				}
			}
		}
		
		Properties shortColNameDict = new Properties();
		for(String shortColName: Discretize.shortColLabelsMap.keySet()){
			String meaning = Discretize.shortColLabelsMap.get(shortColName);
			shortColNameDict.put(meaning, shortColName.replaceAll("\\.csv", "_"));
		}
		
		OutputStream out = FileUtils.openOutputStream(FileUtils.getFile(outPath, "short-col-names.properties"));
		try{
		shortColNameDict.store(out, "");
		}finally{
			out.flush();
			out.close();
		}
	}
}
