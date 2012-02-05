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
	public PerUserDistinctValues(CalcPerUserStats master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws IOException {
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
			String freqFileName = "counts-"
					+ FilenameUtils.removeExtension(dataFile.getName()) + "-"
					+ key + ".csv";
			Writer freqWriter = acquireWriter(freqFileName, master.freqWriterMap, master.freqWriterLocks);
			try{
			Frequency freq = result.get(key);
			
			int uniqueCount = freq.getUniqueCount();
			
			delta = System.currentTimeMillis();
			freqWriter.append(userid).append('\t')
					.append(Integer.toString(uniqueCount))
					.append('\t').append('[');
			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);

			Iterator<Comparable<?>> vIter = freq.valuesIterator();
			while (vIter.hasNext()) {
				String val = (String) vIter.next();
				
				long valCount = freq.getCount(val);
				double valPct = freq.getPct(val);
				
				delta = System.currentTimeMillis();
				freqWriter.append(val).append(':')
						.append(Long.toString(valCount)).append('(')
						.append(Double.toString(valPct))				//Bad idea, we need all precisionString.format("%.6f", freq.getPct(val))) 
						.append("), ");
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_WRITE, delta);
			}
			
			delta = System.currentTimeMillis();
			freqWriter.append("]\n");
			delta = System.currentTimeMillis() - delta;
			PerfMon.increment(TimeMetrics.IO_WRITE, delta);

			}finally{
				releaseWriter(freqFileName, master.freqWriterLocks);
//			synchronized (master.freqWriterMap) {
//				master.freqWriterMap.put(freqFileName, freqWriter);
//			}
			}
		}
	}

	protected String getHeaderLine() {
		return "userid\tdistinct-count\tvalues\n";
	}
}