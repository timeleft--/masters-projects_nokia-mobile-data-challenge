package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;

public class PerUserDistinctValues_wlan extends PerUserDistinctValues {
	@Deprecated
	public PerUserDistinctValues_wlan(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws IOException {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);
	}

	@Override
	protected void delimiterProcedurePrep() {
		if ("rx".equals(currKey)) {
			try {
				// discritize:
				int rssi = Integer.parseInt(currValue);

				// 0 to 5 where 0 is very strong
				rssi = (rssi / 20)-1;

				currValue = rssi + "";

			} catch (NumberFormatException e) {
				// never mind.. it should be NaN
			}
		}
		super.delimiterProcedurePrep();
	}

}
