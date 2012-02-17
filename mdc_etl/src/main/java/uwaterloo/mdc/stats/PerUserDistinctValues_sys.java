package uwaterloo.mdc.stats;

import java.io.File;

public class PerUserDistinctValues_sys extends PerUserDistinctValues {

	private static final int MBYTE = 1024 * 1024;
	private static final int SECS_IN_MIN = 60*60;

	@Deprecated
	public PerUserDistinctValues_sys(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}

	@Override
	protected void delimiterProcedurePrep() {
		try {
			if ("battery".equals(currKey)) {

				// discritize:
				int batPCtg = Integer.parseInt(currValue);
				if(batPCtg > 100){
					currValue = "NaN"; //rejected
				} else {
					batPCtg /= 20;
					currValue = "" + batPCtg;
				}

			} else if(currKey.startsWith("free")){
				int free = Integer.parseInt(currValue);
				
				if(free < 5*MBYTE){
					currValue = 0+ ""; //"low";
				} else {
					currValue = 1+ ""; //"okay";
				}
			} else if("inactive".equals(currKey)){
				int secsInactive = Integer.parseInt(currValue);
				
				if(secsInactive < SECS_IN_MIN){
					currValue = 0+""; //"active";
				} else if(secsInactive < 5 * SECS_IN_MIN){
					currValue = 1+""; //"short";
				}  else if(secsInactive < 15 * SECS_IN_MIN){
					currValue = 2 +""; //"medium";
				} else if(secsInactive < 60 * SECS_IN_MIN){
					currValue = 3 +""; //"long";
				} else {
					currValue = 4 + ""; //"exagerated";
				}
			}
		} catch (NumberFormatException e) {
			// never mind.. it should be NaN
		}
		super.delimiterProcedurePrep();
	}

}
