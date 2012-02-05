package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;

public class PerUserDistinctValues extends CallableOperation<Frequency> {

	@Deprecated
	public PerUserDistinctValues(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws IOException {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}

	protected void headerDelimiterProcedure() {
		result.put(currValue, new Frequency());
	}

	protected void delimiterProcedure() {
		result.get(currKey).addValue(currValue);
	}

	protected void writeResults() throws IOException {
		long delta;
		for (String key : this.getKeyList()) {
			if (Config.USERID_COLNAME.equals(key)) {
				continue;
			}

			String freqFileName = "counts_" + userid + "-"
					+ FilenameUtils.removeExtension(dataFile.getName()) + "-"
					+ key + ".csv";
			Writer freqWriter = acquireWriter(freqFileName,
					master.freqWriterMap, master.freqWriterLocks);
			try {
				Frequency freq = result.get(key);

				// How about a footer line.. or a preheader??? HMMM!!
				// int uniqueCount = freq.getUniqueCount();
				// delta = System.currentTimeMillis();
				// freqWriter.append(userid).append('\t')
				// .append(Integer.toString(uniqueCount))
				// .append('\t').append('[');
				// delta = System.currentTimeMillis() - delta;
				// PerfMon.increment(TimeMetrics.IO_WRITE, delta);

				Iterator<Comparable<?>> vIter = freq.valuesIterator();
				while (vIter.hasNext()) {
					String val = (String) vIter.next();

					long valCount = freq.getCount(val);
					double valPct = freq.getPct(val);

					delta = System.currentTimeMillis();
					freqWriter.append(userid).append('\t').append(val)
							.append('\t').append(Long.toString(valCount))
							.append('\t').append(Double.toString(valPct))
							.append('\n');
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.IO_WRITE, delta);
				}
			} finally {
				releaseWriter(freqWriter, freqFileName, master.freqWriterMap,
						master.freqWriterLocks);
			}
		}

	}

	protected String getHeaderLine() {
		return "userid\tvalue\tcount\tpctage\n";
	}
}