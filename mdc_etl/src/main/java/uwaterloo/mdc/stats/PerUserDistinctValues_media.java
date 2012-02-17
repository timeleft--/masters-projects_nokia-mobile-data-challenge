package uwaterloo.mdc.stats;

import java.io.File;

public class PerUserDistinctValues_media extends PerUserDistinctValues {

	private static final int KBYTE = 1024;

	@Deprecated
	public PerUserDistinctValues_media(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws Exception {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected void delimiterProcedurePrep() {
		if ("size".equals(currKey)) {
			try {
				// discritize:
				int size = Integer.parseInt(currValue);

				if (size < KBYTE) {
					currValue = 0+""; //"tiny";
				} else if (size < 20*KBYTE) {
					currValue = 1+""; //"small";
				} else if (size < 100*KBYTE) {
					currValue = 2+""; //"medium";
				} else if(size < 5000*KBYTE){
					currValue = 3+""; //"large";
				} else {
					currValue = 4+""; //"huge";					
				}

			} catch (NumberFormatException e) {
				// never mind.. it should be NaN
			}
		}
		super.delimiterProcedurePrep();
	}
}
