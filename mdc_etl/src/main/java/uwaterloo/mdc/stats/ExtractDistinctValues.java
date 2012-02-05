package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.operations.CallableOperation;

public class ExtractDistinctValues extends CallableOperation<Frequency> {

	public ExtractDistinctValues(CalcPerUserStats master, char delimiter, String eol,
			int bufferSize, File dataFile, String outPath) throws IOException {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}

	public ExtractDistinctValues(CalcPerUserStats master, File dataFile, String outPath)
			throws IOException {
		this(master, DEFAULT_DELIMITER, DEFAULT_EOL, DEFAULT_BUFF_SIZE,
				dataFile, outPath);
	}

	protected void headerDelimiterProcedure() {
		result.put(currValue, new Frequency());
	}

	protected void delimiterProcedure() {
		result.get(currKey).addValue(currValue);
	}

	protected void writeResults() throws IOException {
		for (String key : this.getKeyList()) {
			if (Config.USERID_COLNAME.equals(key)) {
				continue;
			}
			String freqFileName = "freq-"
					+ FilenameUtils.removeExtension(dataFile.getName()) + "-"
					+ key + ".csv";
			Writer freqWriter = acquireWriter(freqFileName, master.freqWriterMap);

			Frequency freq = result.get(key);

			freqWriter.append(userid).append('\t')
					.append(Integer.toString(freq.getUniqueCount()))
					.append('\t').append('[');

			Iterator<Comparable<?>> vIter = freq.valuesIterator();
			while (vIter.hasNext()) {
				String val = (String) vIter.next();
				freqWriter.append(val).append(':')
						.append(Long.toString(freq.getCount(val))).append('(')
						.append(String.format("%.6f", freq.getPct(val)))
						.append("), ");
			}
			freqWriter.append("]\n");
			synchronized (master.freqWriterMap) {
				master.freqWriterMap.put(freqFileName, freqWriter);
			}
		}
	}

	protected String getHeaderLine() {
		return "userid\tdistinct-count\tvalues\n";
	}
}