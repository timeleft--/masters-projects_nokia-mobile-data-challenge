package uwaterloo.mdc.etl.operations;

import java.io.Writer;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize;
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

	private final HashMap<String, Writer> statWriters;
	private final String statsPath;

	public PrintStatsCallable(Object statObj, String userid, String statKey,
			HashMap<String, Writer> statWriters, String statsPath) {
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

		String filename = PERUSER_SUMMART_PREFX + statKey + ".csv";
		Writer summaryWriter = ConcurrUtil
				.acquireWriter(
						statsPath,
						filename,
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
			ConcurrUtil.releaseWriter(summaryWriter, filename, statWriters,
					false);
		}
	}

	private void writeStats(String userid, String statKey, Frequency stat)
			throws Exception {

		Enum<?>[] valsArr;
		synchronized (Discretize.enumsMap) {
			valsArr = Discretize.enumsMap.get(statKey);
		}
		if (valsArr == null) {
			if (statKey.endsWith(Config.RESULT_KEY_READING_NOVISIT_FREQ)) {
				valsArr = Discretize.VisitReadingBothEnum.values();
			} else {
				// leave it to cause a null pointer exception!
			}
		}

		StringBuilder headerBuilder = new StringBuilder();
		headerBuilder.append(StringUtils.quote(Config.USERID_COLNAME));

		for (int i = 0; i < valsArr.length; ++i) {
			Enum<?> val = valsArr[i];
			String valLabel = val.toString();
			headerBuilder.append('\t')
					.append(StringUtils.quote(valLabel + COUNT_PSTFX))
					.append('\t')
					.append(StringUtils.quote(valLabel + PCTG_PSTFX));
		}
		
		headerBuilder.append('\t').append("count_total");

		headerBuilder.append('\n');

		String filename = PERUSER_FREQ_PREFX + statKey + ".csv";

		Writer freqWriter = ConcurrUtil.acquireWriter(statsPath, filename,
				statWriters, headerBuilder.toString());
		try {
			long delta = System.currentTimeMillis();

			freqWriter.append(userid);

			for (int i = 0; i < valsArr.length; ++i) {
				Enum<?> val = valsArr[i];

				long valCnt = stat.getCount(val);
				double valPct = stat.getPct(val);

				freqWriter.append('\t').append(Long.toString(valCnt))
						.append('\t').append(Double.toString(valPct));

			}

			freqWriter.append('\t').append(Long.toString(stat.getUniqueCount()));
			
			freqWriter.append('\n');

			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);
		} finally {
			ConcurrUtil.releaseWriter(freqWriter, filename, statWriters, false);
		}
	}
}
