package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.MathUtil;

public class LoadInputsIntoDocs_bluetooth extends LoadInputsIntoDocs {

	private Frequency macFreq = new Frequency();
	private SummaryStatistics btDeviceCount = new SummaryStatistics();
	private Frequency btDeviceCountFreq = new Frequency();

	protected void onMicroLocChange() {
		StringBuilder colBuider = colOpResult.get("mac_address");
		double mean = btDeviceCount.getMean();
		colBuider.append(" avgbt" + mean).append(
				" sdvbt" + btDeviceCount.getStandardDeviation());
		btDeviceCountFreq.addValue(mean);

		btDeviceCount = new SummaryStatistics();
		macFreq = new Frequency();

		super.onTimeChanged();
	}

	@Override
	protected void onTimeChanged() {
		btDeviceCount.addValue(macFreq.getUniqueCount());

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
		//TODO: Debug, why does this result in huge numbers?
		macFreq.addValue(currValue);

		if (Config.USER_SPECIFIC_FEATURES) {
			long encounters = macFreq.getCount(currValue);

			if (MathUtil.getPow2(encounters) < 0) {
				// In case of num > 1024, that's a stop word!
				return null;
			}
			long lgEnc = MathUtil.tf(encounters);

			return Long.toString(lgEnc) + " bt" + currValue.toString(); // Mac
																		// Address
		} else {
			return null;
		}
	}

	@Override
	protected boolean keepContinuousStatsForColumn(String colName) {
		return false;
	}

	@Override
	protected KeyValuePair<String, HashMap<String, Object>> getReturnValue()
			throws Exception {

		KeyValuePair<String, HashMap<String, Object>> result = super
				.getReturnValue();

		result.getValue()
				.put(Config.RESULT_KEY_AVG_BTS_FREQ, btDeviceCountFreq);
		return result;
	}

}
