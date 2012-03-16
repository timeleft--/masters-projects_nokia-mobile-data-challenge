package uwaterloo.mdc.stats;

import java.io.File;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.mallet.CountConditionalFreqs;
import uwaterloo.mdc.etl.mallet.ImportIntoMallet;
import uwaterloo.mdc.etl.mallet.ImportIntoMallet.MalletImportedFiles;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.ConcurrUtil;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.weka.ClassifyAndFeatSelect;
import uwaterloo.mdc.weka.LoadCountsAsAttributes;

class CalcQuantizationBoundaries {

	// private static final String EMPTY_COUNTS_LINE =
	// "\t"+Config.MISSING_VALUE_PLACEHOLDER+"\t"+Config.MISSING_VALUE_PLACEHOLDER+"\t"+Config.MISSING_VALUE_PLACEHOLDER;

	private String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private String outPath = "P:\\mdc-datasets\\quantization";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO dataRoot from args

		CalcQuantizationBoundaries app = new CalcQuantizationBoundaries();
		app.quantize();

		ImportIntoMallet.main(args);
		
		LoadCountsAsAttributes.main(args);
		
		CountConditionalFreqs countCond = new CountConditionalFreqs();
		ExecutorService countExec = Executors.newSingleThreadExecutor();
		countExec.submit(countCond);

		countExec.shutdown();
		while (!countExec.isTerminated()) {
			Thread.sleep(5000);
		}
		
		ClassifyAndFeatSelect.main(args);		
	}

	private Map<String, Writer> quantileWriterMap = Collections
			.synchronizedMap(new HashMap<String, Writer>());
	private Map<String, SummaryStatistics> p25SummaryMap = Collections
			.synchronizedMap(new HashMap<String, SummaryStatistics>());
	private Map<String, SummaryStatistics> p60SummaryMap = Collections
			.synchronizedMap(new HashMap<String, SummaryStatistics>());
	private Map<String, SummaryStatistics> p80SummaryMap = Collections
			.synchronizedMap(new HashMap<String, SummaryStatistics>());
	private Map<String, SummaryStatistics> p95SummaryMap = Collections
			.synchronizedMap(new HashMap<String, SummaryStatistics>());
	private Map<String, SummaryStatistics> countSummaryMap = Collections
			.synchronizedMap(new HashMap<String, SummaryStatistics>());
	

	private void quantize() throws Exception {
		try {
			// To make sure the class is loaded
			System.out.println(PerfMon.asString());

			CallableOperationFactory<KeyValuePair<String, HashMap<String, Percentile>>, LinkedList<Double>> factory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Percentile>>, LinkedList<Double>>();
			LinkedList<Future<KeyValuePair<String, HashMap<String, Percentile>>>> quantileFutures = new LinkedList<Future<KeyValuePair<String, HashMap<String, Percentile>>>>();
			ExecutorService exec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			// int numberTasks = 0;

			File dataRootFile = FileUtils.getFile(dataRoot);
			int userIx = 0;
			for (File userDir : dataRootFile.listFiles()) {
				if (userIx == Config.NUM_USERS_TO_PROCESS) {
					break;
				}
				for (File dataFile : userDir.listFiles(new MalletImportedFiles())) { 

					PerUserQuantiles quantilesCall = (PerUserQuantiles) factory
							.createOperation(PerUserQuantiles.class, this,
									dataFile, outPath);

					quantileFutures.add(exec.submit(quantilesCall));
					// ++numberTasks;
				}
				++userIx;
			}

			ExecutorService printExec = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			LinkedList<Future<Void>> printFutures = new LinkedList<Future<Void>>();
			for (Future<KeyValuePair<String, HashMap<String, Percentile>>> future : quantileFutures) {
				KeyValuePair<String, HashMap<String, Percentile>> quantileMap = future
						.get();
				if (quantileMap == null) {
					System.err.println("Null quantile map");
					continue;
				}

				for (String fnamePfx : quantileMap.getValue().keySet()) {
					PrintQuantileCall printCall = new PrintQuantileCall(
							quantileMap, fnamePfx);
					printFutures.add(printExec.submit(printCall));
				}

			}

			for (Future<Void> pf : printFutures) {
				pf.get();
			}

		
			Properties quantizedProps = new Properties();
			
			for (String summaryKey : p25SummaryMap.keySet()) {
				String fnameSuffix = summaryKey + "_quantiles.csv";
				quantizedProps.setProperty(summaryKey, fnameSuffix);
				SummaryStatistics summary25 = p25SummaryMap.get(summaryKey);
				SummaryStatistics summary60 = p60SummaryMap.get(summaryKey);
				SummaryStatistics summary80 = p80SummaryMap.get(summaryKey);
				SummaryStatistics summary95 = p95SummaryMap.get(summaryKey);
				String filePath = FilenameUtils.concat(outPath, "mean_"
						+ fnameSuffix);
				Writer summaryWr = Channels.newWriter(FileUtils
						.openOutputStream(FileUtils.getFile(filePath))
						.getChannel(), Config.OUT_CHARSET);
				double avgCount = countSummaryMap.get(summaryKey).getMean();
				try {
					summaryWr
//							.append("25-ptile\t60-ptile\t80-ptile\t95-ptile\n")
							.append(Double.toString(summary25.getMean()/avgCount))
							.append('\t')
							.append(Double.toString(summary60.getMean()/avgCount))
							.append('\t')
							.append(Double.toString(summary80.getMean()/avgCount))
							.append('\t')
							.append(Double.toString(summary95.getMean()/avgCount))
							.append('\n');
				} finally {
					summaryWr.flush();
					summaryWr.close();
				}
				// debug
				FileUtils.writeStringToFile(
						FileUtils.getFile(outPath, "summary25_" + summaryKey
								+ "_quantiles.csv"), summary25.toString());
				FileUtils.writeStringToFile(
						FileUtils.getFile(outPath, "summary60_" + summaryKey
								+ "_quantiles.csv"), summary60.toString());
				FileUtils.writeStringToFile(
						FileUtils.getFile(outPath, "summary80_" + summaryKey
								+ "_quantiles.csv"), summary80.toString());
				FileUtils.writeStringToFile(
						FileUtils.getFile(outPath, "summary95_" + summaryKey
								+ "_quantiles.csv"), summary95.toString());
			}
			Writer quantizedPropsWr = Channels.newWriter(FileUtils
					.openOutputStream(FileUtils.getFile(Config.QUANTIZED_FIELDS_PROPERTIES))
					.getChannel(), Config.OUT_CHARSET);
			try{
			quantizedProps.store(quantizedPropsWr, null);
		}finally{
			quantizedPropsWr.flush();
			quantizedPropsWr.close();
		}
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			exec.shutdown();
			// Wait until all threads are finish
			while (!exec.isTerminated()) {
				Thread.sleep(5000);
				System.out.println(PerfMon.asString());
			}
		} finally {
//			long delta = System.currentTimeMillis();
//			for (Writer wr : quantileWriterMap.values()) {
//				// This is just in case the program crashed
//				if (wr != null) {
//					wr.flush();
//					wr.close();
//				}
//			}
//			delta = System.currentTimeMillis() - delta;
//			PerfMon.increment(TimeMetrics.IO_WRITE, delta);

			System.out.println(PerfMon.asString());
			System.out.println("Done!");
		}
	}

	private class PrintQuantileCall implements Callable<Void> {

		private final KeyValuePair<String, HashMap<String, Percentile>> quantileMap;
		private final String fnamePfx;

		public PrintQuantileCall(
				KeyValuePair<String, HashMap<String, Percentile>> quantileMap,
				String fnamePfx) {
			this.quantileMap = quantileMap;
			this.fnamePfx = fnamePfx;
		}

		@Override
		public Void call() throws Exception {

			Percentile qt = quantileMap.getValue().get(fnamePfx);
			
			double p25 = qt.evaluate(25);
			double p60 = qt.evaluate(60);
			double p80 = qt.evaluate(80);
			double p95 = qt.evaluate(95);

			
			String filePath = FilenameUtils.concat(outPath,
					quantileMap.getKey() + "_" + fnamePfx + "_quantiles.csv");

			Writer quantileWr = ConcurrUtil.acquireWriter(filePath,
					quantileWriterMap, "");
//					// "userid\t" +
//					"25-ptile\t60-ptile\t80-ptile\t95-ptile\n");
			try {
				quantileWr
						.
						// append(quantileMap.getKey()).append('\t')
						append(Double.toString(p25)).append('\t')
						.append(Double.toString(p60)).append('\t')
						.append(Double.toString(p80)).append('\t')
						.append(Double.toString(p95)).append('\n');
			} finally {
				ConcurrUtil.releaseWriter(quantileWr, filePath,
						quantileWriterMap, true); //false
				
			}
			
			int readingCount = qt.getData().length;
			
			p25 *= readingCount;
			p60 *= readingCount;
			p80 *= readingCount;
			p95 *= readingCount;
			
			synchronized (countSummaryMap) {
				SummaryStatistics countSummary = countSummaryMap.get(fnamePfx);
				if (countSummary == null) {
					countSummary = new SummaryStatistics();
					countSummaryMap.put(fnamePfx, countSummary);
				}
				countSummary.addValue(readingCount);
			}
			
			synchronized (p25SummaryMap) {
				SummaryStatistics p25Summary = p25SummaryMap.get(fnamePfx);
				if (p25Summary == null) {
					p25Summary = new SummaryStatistics();
					p25SummaryMap.put(fnamePfx, p25Summary);
				}
				p25Summary.addValue(p25);
			}
			synchronized (p60SummaryMap) {
				SummaryStatistics p60Summary = p60SummaryMap.get(fnamePfx);
				if (p60Summary == null) {
					p60Summary = new SummaryStatistics();
					p60SummaryMap.put(fnamePfx, p60Summary);
				}
				p60Summary.addValue(p60);
			}

			synchronized (p80SummaryMap) {
				SummaryStatistics p80Summary = p80SummaryMap.get(fnamePfx);
				if (p80Summary == null) {
					p80Summary = new SummaryStatistics();
					p80SummaryMap.put(fnamePfx, p80Summary);
				}
				p80Summary.addValue(p80);
			}

			synchronized (p95SummaryMap) {
				SummaryStatistics p95Summary = p95SummaryMap.get(fnamePfx);
				if (p95Summary == null) {
					p95Summary = new SummaryStatistics();
					p95SummaryMap.put(fnamePfx, p95Summary);
				}
				p95Summary.addValue(p95);
			}

			

			return null;
		}

	}

}
