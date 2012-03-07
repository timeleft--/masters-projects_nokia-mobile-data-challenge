package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashSet;

import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.util.MathUtil;

public class LoadInputsIntoDocs_bluetooth extends LoadInputsIntoDocs {

	private Frequency macFreq = new Frequency();
	
	protected void onMicroLocChange() {
		macFreq = new Frequency();
	}
	
	public LoadInputsIntoDocs_bluetooth(Object master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}
	
	private static HashSet<String> colsToSkip = new HashSet<String>();
	static {
		// TODO: Remove when supporting OUI 
		colsToSkip.add("mac_prefix");
		colsToSkip.add("name");
	}
	
	@Override
	public HashSet<String> getColsToSkip() {
		return colsToSkip;
	}


	@Override
	protected Comparable<?> getValueToWrite() {
		macFreq.addValue(currValue);
		long encounters = macFreq.getCount(currValue);

		if(MathUtil.getPow2(encounters) <0){
			// In case of num > 1024, that's a stop word!
			return null;
		}
		long lgEnc = MathUtil.lgSmoothing(encounters);
		
		return Long.toString(lgEnc) + "B" + currValue.toString(); //Mac Address
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

}
