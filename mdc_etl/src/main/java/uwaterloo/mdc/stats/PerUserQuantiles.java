package uwaterloo.mdc.stats;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import uwaterloo.mdc.etl.operations.CallableOperation;
import uwaterloo.mdc.etl.util.KeyValuePair;

public class PerUserQuantiles extends
		CallableOperation<KeyValuePair<String,HashMap<String,Percentile>>, LinkedList<Double>> {
	
	@SuppressWarnings("deprecation")
	public PerUserQuantiles(CalcQuantizationBoundaries master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}

	protected void headerDelimiterProcedure() {
		colOpResult.put(currValue, new LinkedList<Double>());
	}
	
	public HashSet<String> getColsToSkip() {
		return new HashSet<>();
	}

	protected void delimiterProcedure() {
		if(getColsToSkip().contains(currKey) || currKey.equals(getTimeColumnName()) || "tz".equals(currKey) || currKey.contains("time")){
			return;
		}
		try{
			Double currNum = Double.parseDouble(currValue);
			if(currNum == 0){
				// 0 is not a numeric reading
				// just a placeholder
				// TODO: right? ;)
				return;
			}
			colOpResult.get(currKey).add(currNum);
		}catch(NumberFormatException ignored){
			// ok.. not a numeric col!
		}
	}

	/**
	 * Since the CSVs of MDC represent time series, the column representing time
	 * must be treated specially.
	 * 
	 * @return The anem of the column representing time
	 */
	protected String getTimeColumnName() {
		return "time";
	}

	protected void writeResults() throws Exception {
	}

	protected String getHeaderLine() {
		return null;
	}

	@Override
	protected void headerDelimiterProcedurePrep() {
		// nothin

	}

	@Override
	protected void delimiterProcedurePrep() {
		// nothin

	}

	@Override
	protected KeyValuePair<String, HashMap<String, Percentile>> getReturnValue() {
		String fnamePfx = FilenameUtils.removeExtension(dataFile.getName()) + "_";
		HashMap<String, Percentile> result = new HashMap<String, Percentile>();
		for(String colName: keyList){
			LinkedList<Double> valList = colOpResult.remove(colName);
			if(valList == null || valList.isEmpty()){
				continue;
			}
			double[] valArr = new double[valList.size()];
			int d = 0;
			while (!valList.isEmpty()) {
				Double val= valList.removeFirst();
				valArr[d++] = val;
			}
			Percentile perc = new Percentile();
			perc.setData(valArr);
			result.put(fnamePfx + colName, perc);
		}
		return new KeyValuePair<String, HashMap<String, Percentile>>(userid, result);
	}

	@Override
	protected void eoFileProcedure() throws Exception {
		// TODO Auto-generated method stub
		
	}
}