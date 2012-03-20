package uwaterloo.mdc.weka;

import java.io.File;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.operations.CallableOperationFactory;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.stats.CalcQuantizationBoundaries;
import uwaterloo.mdc.stats.PerUserQuantiles;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Discretize;

public class CalcCutPoints extends CalcQuantizationBoundaries implements
		Callable<Void> {

	private static String dataRoot = "P:\\mdc-datasets\\mdc2012-375-taskdedicated";
	private static String outPath = "C:\\mdc-datasets\\weka\\cutpoints";

	private final File[] inputFiles;

	public CalcCutPoints(File[] inputFiles) {
		this.inputFiles = inputFiles;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(new Date() + ": Started");
		File dataDir = FileUtils.getFile(dataRoot);

		File[] accel = new File[Config.NUM_USERS_TO_PROCESS];
		File[] calllog = new File[Config.NUM_USERS_TO_PROCESS];
		File[] sys = new File[Config.NUM_USERS_TO_PROCESS];

//Nothing to quantize belwo	
//		File[] application = new File[Config.NUM_USERS_TO_PROCESS];
//		File[] bluetooth = new File[Config.NUM_USERS_TO_PROCESS];
//		File[] calendar = new File[Config.NUM_USERS_TO_PROCESS];
//		File[] mediaplay = new File[Config.NUM_USERS_TO_PROCESS];
		
		File[] userDirs = dataDir.listFiles();
		for (int u = 0; u < Config.NUM_USERS_TO_PROCESS; ++u) {
			accel[u] = FileUtils.getFile(userDirs[u], "accel.csv");
			calllog[u] = FileUtils.getFile(userDirs[u], "calllog.csv");
			sys[u] = FileUtils.getFile(userDirs[u], "sys.csv");
			
//			application[u] = FileUtils.getFile(userDirs[u], "application.csv");
//			bluetooth[u] = FileUtils.getFile(userDirs[u], "bluetooth.csv");
//			calendar[u] = FileUtils.getFile(userDirs[u], "calendar.csv");
//		mediaplay[u] = FileUtils.getFile(userDirs[u], "mediaplay.csv");
			
		}

		// Common sense says that I shouldn't try parallelizing all this data
		CalcCutPoints app = new CalcCutPoints(accel);
		app.call();

//		app = new CalcCutPoints(application);
//		app.call();

//		app = new CalcCutPoints(bluetooth);
//		app.call();
//
//		app = new CalcCutPoints(calendar);
//		app.call();

		app = new CalcCutPoints(calllog);
		app.call();

//		app = new CalcCutPoints(mediaplay);
//		app.call();

		app = new CalcCutPoints(sys);
		app.call();

		System.out.println(new Date() + ": Ended");
	}

	@Override
	public Void call() throws Exception {
		CallableOperationFactory<KeyValuePair<String, HashMap<String, Percentile>>, LinkedList<Double>> factory = new CallableOperationFactory<KeyValuePair<String, HashMap<String, Percentile>>, LinkedList<Double>>();
		LinkedList<Future<KeyValuePair<String, HashMap<String, Percentile>>>> quantileFutures = new LinkedList<Future<KeyValuePair<String, HashMap<String, Percentile>>>>();
		ExecutorService exec = Executors.newFixedThreadPool(Config.NUM_THREADS);
		// int numberTasks = 0;
		for (File dataFile : inputFiles) {

			PerUserQuantiles quantilesCall = (PerUserQuantiles) factory
					.createOperation(PerUserQuantiles.class, this, dataFile,
							outPath);

			quantileFutures.add(exec.submit(quantilesCall));
			// ++numberTasks;
		}

		HashMap<String, Filter> discretizers = new HashMap<String, Filter>();
		HashMap<Filter, Instances> structMap = new HashMap<Filter, Instances>();
		int userIx = 0;
		for (Future<KeyValuePair<String, HashMap<String, Percentile>>> future : quantileFutures) {
			KeyValuePair<String, HashMap<String, Percentile>> quantileMap = future
					.get();
			if (quantileMap == null) {
				System.err.println("Null quantile map");
				continue;
			}
//			if(quantileMap.getKey() == null){
//				// empty file.. do nothing
//			}
			System.out.println(new Date() + ": Finished reading "
					+ quantileMap.getKey() );
			for (String fnamePfx : quantileMap.getValue().keySet()) {

				Instances dataset;
				Discretize filter = (Discretize) discretizers.get(fnamePfx);
				if (filter == null) {

					filter = new Discretize();
					discretizers.put(fnamePfx, filter);
					// Supervised

					// Needs class labels: filter.setUseKononenko(true);
					// filter.setUseBetterEncoding(true);

					// Unsupervised
					filter.setBins(Config.NUM_QUANTILES + 1);
					// filter.setIgnoreClass(true)
					// GOOOOD
					 filter.setUseEqualFrequency(true);
					// or this 
//					 filter.setFindNumBins(true);

					// both
					filter.setAttributeIndices("1");
					// filter.setMakeBinary(Config.SPREAD_NOMINAL_FEATURES_AS_BINARY);

					FastVector struct = new FastVector(1); // + 1);
					struct.addElement(new Attribute(fnamePfx));
					// FastVector dummLabelling = new FastVector();
					// dummLabelling.addElement("+1");
					// dummLabelling.addElement("-1");
					// Attribute dummyCls = new Attribute("dummy-label",
					// dummLabelling);
					// struct.addElement(dummyCls);
					dataset = new Instances(fnamePfx, struct, 0);
					// dataset.setClassIndex(1);
					filter.setInputFormat(dataset);
					structMap.put(filter, dataset);
				}

				dataset = structMap.get(filter);

				Percentile qt = quantileMap.getValue().get(fnamePfx);
				double[] data = qt.getData();

				for (double d : data) {
					Instance inst = new Instance(1 + 1);
					inst.setDataset(dataset);
					inst.setValue(0, d);
					filter.input(inst);
				}
				System.out.println(new Date() + ": Finished loading "
						+ fnamePfx );

			}
			++userIx;
		}

		for (String fnamePfx : discretizers.keySet()) {
			Discretize filter = (Discretize) discretizers.get(fnamePfx);
			filter.batchFinished();
			double[] cutPoints = filter.getCutPoints(0);
			// System.out.println(filter.output());
			if (cutPoints != null) {
				String filePath = FilenameUtils.concat(outPath, "weka_"
						+ fnamePfx + "_quantiles.csv");

				Writer quantileWr = Channels.newWriter(FileUtils
						.openOutputStream(FileUtils.getFile(filePath))
						.getChannel(), Config.OUT_CHARSET);
				try {
//					quantileWr.append(Integer.toString(cutPoints.length));
					for (double cp : cutPoints) {
						quantileWr.append(Double.toString(cp)).append('\t');
					}
					quantileWr.append('\n');

				} finally {
					quantileWr.flush();
					quantileWr.close();
				}
			}
			System.out.println(new Date() + ": Calculated " + fnamePfx);
		}

		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		exec.shutdown();
		// Wait until all threads are finish
		while (!exec.isTerminated()) {
			Thread.sleep(5000);
//			System.out.println("Shutting down!");
		}

		return null;
	}

}
