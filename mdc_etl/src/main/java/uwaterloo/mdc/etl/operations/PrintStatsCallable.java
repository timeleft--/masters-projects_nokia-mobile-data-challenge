package uwaterloo.mdc.etl.operations;

import java.io.Writer;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.Discretize.ReadingWithinVisitEnum;
import uwaterloo.mdc.etl.Discretize.VisitWithReadingEnum;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.util.ConcurrUtil;
import uwaterloo.mdc.etl.util.StringUtils;

public class PrintStatsCallable implements Callable<Void> {
	private static final String COUNT_PSTFX = "_count";
	private static final String PCTG_PSTFX = "_pctg";
	private static final String PERUSER_FREQ_PREFX = "per-user-freq_";
	private static final String PERUSER_SUMMART_PREFX = "per-user-summary_";

	private final Object statObj;
	private final String userid;
	private final String statKey;

	private final Map<String, Writer> statWriters;
	private final String statsPath;

	public PrintStatsCallable(Object statObj, String userid, String statKey,
			Map<String, Writer> statWriters, String statsPath) {
		super();
		this.statObj = statObj;
		this.userid = userid;
		this.statKey = statKey;
		this.statWriters = statWriters;
		this.statsPath = statsPath;
	}

	public Void call() throws Exception {
		if (statObj instanceof Frequency) {
			writeStats(userid, statKey, ((Frequency) statObj));

		} else if (statObj instanceof SummaryStatistics) {
			writeStats(userid, statKey, (SummaryStatistics) statObj);

		}
		return null;
	}

	private void writeStats(String userid, String statKey,
			SummaryStatistics stat) throws Exception {

		String filePath = FilenameUtils.concat(statsPath, PERUSER_SUMMART_PREFX
				+ statKey + ".csv");
		Writer summaryWriter = ConcurrUtil
				.acquireWriter(
						filePath,
						statWriters,
						StringUtils.quote(Config.USERID_COLNAME)
								+ "\t\"n\"\t\"min\"\t\"max\"\t\"mean\"\t\"standard_deviation\"\t\"variance\"\t\"geometric_mean\"\t\"second_moment\"\n");
		try {
			long delta = System.currentTimeMillis();
			summaryWriter.append(userid).append('\t').append("" + stat.getN())
					.append('\t').append("" + stat.getMin()).append('\t')
					.append("" + stat.getMax()).append('\t')
					.append("" + stat.getMean()).append('\t')
					.append("" + stat.getStandardDeviation()).append('\t')
					.append("" + stat.getVariance()).append('\t')
					.append("" + stat.getGeometricMean()).append('\t')
					.append("" + stat.getSecondMoment()).append('\n');

			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);
		} finally {
			ConcurrUtil.releaseWriter(summaryWriter, filePath, statWriters,
					false);
		}
	}

	private void writeStats(String userid, String statKey, Frequency stat)
			throws Exception {

		Comparable<?>[] valsArr;
		synchronized (Discretize.enumsMap) {
			if (Config.QUANTIZE_NOT_DISCRETIZE) {
				if (Discretize.enumsMap.containsKey(statKey)) {
					valsArr = Discretize.QuantilesEnum.values();
					assert Config.NUM_QUANTILES + 1 == valsArr.length : "Generalize this code.. but don't use digits only";
				} else {
					valsArr = null;
				}
			} else {
				valsArr = Discretize.enumsMap.get(statKey);
			}
		}
		if (valsArr == null) {
			if (statKey.endsWith(Config.RESULT_KEY_READING_NOVISIT_FREQ)) {
				valsArr = ReadingWithinVisitEnum.values();
			} else if (statKey.endsWith(Config.RESULT_KEY_VISIT_NOREADING_FREQ)) {
				valsArr = VisitWithReadingEnum.values();
			} else if (statKey.equals(Config.RESULT_KEY_LOCATIONS_PER_USER)) {
				// The user location has no preset value enumeration
				// So use the values of user 14 who has 54 distinct ids
				valsArr = new String[55];
				for (int i = 0; i < valsArr.length; ++i) {
					valsArr[i] = userid + "_" + (i + 1);
				}
			} else if (statKey.endsWith(Config.RESULT_POSTFX_INTEGER)) {
				valsArr = new Integer[55];
				for (int i = 0; i < valsArr.length; ++i) {
					valsArr[i] = i + 1;
				}
			}
		}

		StringBuilder headerBuilder = new StringBuilder();
		headerBuilder.append(StringUtils.quote(Config.USERID_COLNAME));

		for (int i = 0; i < valsArr.length; ++i) {
			Comparable<?> val = valsArr[i];
			String valLabel = val.toString();
			headerBuilder.append('\t')
					.append(StringUtils.quote(valLabel + COUNT_PSTFX))
					.append('\t')
					.append(StringUtils.quote(valLabel + PCTG_PSTFX));
		}

		headerBuilder.append('\t').append(
				StringUtils.quote("total" + COUNT_PSTFX));

		headerBuilder.append('\n');

		String filePath = FilenameUtils.concat(statsPath, PERUSER_FREQ_PREFX
				+ statKey + ".csv");

		Writer freqWriter = ConcurrUtil.acquireWriter(filePath, statWriters,
				headerBuilder.toString());
		try {
			long delta = System.currentTimeMillis();

			freqWriter.append(userid);

			for (int i = 0; i < valsArr.length; ++i) {
				Comparable<?> val = valsArr[i];

				long valCnt = stat.getCount(val);
				double valPct = stat.getPct(val);

				freqWriter.append('\t').append(Long.toString(valCnt))
						.append('\t').append(Double.toString(valPct));

			}

			freqWriter.append('\t').append(Long.toString(stat.getSumFreq()));

			freqWriter.append('\n');

			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);
		} finally {
			ConcurrUtil.releaseWriter(freqWriter, filePath, statWriters, false);
		}
	}
}
