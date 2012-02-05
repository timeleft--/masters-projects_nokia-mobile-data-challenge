package uwaterloo.mdc.stats;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.etl.operations.CallableOperation;

public class PerUserDistinctValues extends CallableOperation<Frequency> {
	public static class ValueCardinality<K> implements Map.Entry<K, Integer>{
		protected K key;
		protected Integer value;
		
		public ValueCardinality(K key, Integer value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		public Integer getValue() {
			return value;
		}

		public Integer setValue(Integer value) {
			this.value = value;
			return value;
		}
		
	}
	private static final String READINGS_AT_SAME_TIME = "time_cardinality_";
	private HashMap<String,ValueCardinality<Long>> prevTimeColsReadings = new HashMap<String,ValueCardinality<Long>>();

	@Deprecated
	public PerUserDistinctValues(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws IOException {
		super(master, delimiter, eol, bufferSize, dataFile, outPath);

	}
	
	@Override
	protected void headerEolProcedure() {
		for(String key: keyList){
			if(key.contains("time")){
				opResult.put(READINGS_AT_SAME_TIME+key, new Frequency());
			}
		}
		super.headerEolProcedure();
	}
	
	protected void headerDelimiterProcedure() {
		opResult.put(currValue, new Frequency());
	}

	protected void delimiterProcedure() {
		opResult.get(currKey).addValue(currValue);
		
		if (currKey.contains("time")) {
			ValueCardinality<Long> prevReading = prevTimeColsReadings.get(READINGS_AT_SAME_TIME+currKey);
			if(prevReading == null){
				// initialization
				prevReading = new ValueCardinality<Long>(new Long(currValue),0);
				prevTimeColsReadings.put(READINGS_AT_SAME_TIME+currKey, prevReading);
			}
			Long longValue = Long.valueOf(currValue);
			if(prevReading.getKey().equals(longValue)){
				//Another reading at the same time
				prevReading.setValue(prevReading.getValue() + 1);
			} else {
				opResult.get(READINGS_AT_SAME_TIME+currKey).addValue(prevReading.getValue().intValue());
				
				prevReading.setKey(longValue);
				prevReading.setValue(1);
			}
		}
	}

	protected void writeResults() throws IOException {
		long delta;
		for (String key : opResult.keySet()) {
			if (Config.USERID_COLNAME.equals(key)) {
				continue;
			}

			String freqFileName = "counts_"
					+ FilenameUtils.removeExtension(dataFile.getName()) + "-"
					+ key + "-" + userid + ".csv";
			Writer freqWriter = acquireWriter(freqFileName,
					master.freqWriterMap, master.freqWriterLocks);
			try {
				Frequency freq = opResult.get(key);

				if (key.contains("time") && !key.startsWith(READINGS_AT_SAME_TIME)) {
					//Time frequencies are not useful and are very hard to calculate
					// because of the large number of different values
					int uniqueCount = freq.getUniqueCount();
					delta = System.currentTimeMillis();
					freqWriter.append(userid).append('\t').append("ALL")
							.append('\t').append(Integer.toString(uniqueCount))
							.append('\t').append("N/A").append('\n');
					delta = System.currentTimeMillis() - delta;
					PerfMon.increment(TimeMetrics.IO_WRITE, delta);

					continue;
				}

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
	
	@Override
	protected void eoFileProcedure() {
		for(String key: opResult.keySet()){
			if(key.startsWith(READINGS_AT_SAME_TIME)){
				ValueCardinality<Long> lastReading = prevTimeColsReadings.get(key);
				opResult.get(key).addValue(lastReading.getValue().intValue());
			}
		}
		
	}

	@Override
	protected void headerDelimiterProcedurePrep() {
		// nothin
		
	}

	@Override
	protected void delimiterProcedurePrep() {
		// nothin
		
	}
}