package uwaterloo.mdc.weka;

import java.io.File;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;

import weka.clusterers.ClusterEvaluation;
import weka.clusterers.Clusterer;
import weka.clusterers.SimpleKMeans;
import weka.clusterers.XMeans;
import weka.core.DistanceFunction;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

public class ClusterByEntropy implements Callable<Void> {

	static class SimpleKMeanStar extends SimpleKMeans {
		@Override
		public void setDistanceFunction(DistanceFunction df) throws Exception {
			m_DistanceFunction = df;
		}
	}

	private static final int NUM_USERS_TO_PROCESS = 20;
	String inPath = "/Users/yia/Dropbox/nokia-mdc/segmented_user/ALL/";
	String outPath = "/Users/yia/nokia-mdc/clustering/";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new ClusterByEntropy().call();
	}

	Instances trainingInsts = null;
	private Instances noClassSet;

	public Void call() throws Exception {
		int fileIx = 0;
		for (File inFile : FileUtils.listFiles(FileUtils.getFile(inPath),
				new String[] { "arff" }, false)) {
			if (fileIx == NUM_USERS_TO_PROCESS) {
				break;
			}
			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(inFile);

			Instances dataStructure = dataLoader.getStructure();
			dataStructure.setClassIndex(dataStructure.numAttributes() - 1);

			if (fileIx == 0) {
				trainingInsts = new Instances(dataStructure);
			}

			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStructure)) != null) {
				trainingInsts.add(dataInst);
			}
			++fileIx;
		}

		Remove remClass = new Remove();
		remClass.setInputFormat(trainingInsts);
		remClass.setAttributeIndices("last"); //
		Integer.toString(trainingInsts.numAttributes());
		noClassSet = Filter.useFilter(trainingInsts, remClass);
		// Work around:
		if (noClassSet.attribute(noClassSet.numAttributes() - 1).name()
				.equals("label")) {
			noClassSet.setClassIndex(-1);
			noClassSet.deleteAttributeAt(noClassSet.numAttributes() - 1);
		}

		Random rand = new Random(System.currentTimeMillis());

		ExecutorService exec = Executors.newFixedThreadPool(2);
		Future<Void> lastFuture;
		int seed = rand.nextInt();

		
		SimpleKMeanStar kmStar = new SimpleKMeanStar();
		kmStar.setNumClusters(7);
		kmStar.setSeed(seed);
		kmStar.setDistanceFunction(new EntropyDistanceMetric(trainingInsts));
		kmStar.setDontReplaceMissingValues(true);

		lastFuture = exec.submit(new ClustererEval(kmStar));

		 XMeans xm = new XMeans();
				 xm.setDistanceF(new EntropyDistanceMetric(trainingInsts));
				 xm.setMaxNumClusters(11);
				 xm.setMinNumClusters(4);
				 xm.setSeed(rand.nextInt());
//		SimpleKMeans km = new SimpleKMeans();
//		km.setNumClusters(7);
//		km.setSeed(seed);
//		kmStar.setDontReplaceMissingValues(true);

		lastFuture = exec.submit(new ClustererEval(xm));

		lastFuture.get();

		exec.shutdown();
		while (!exec.isTerminated()) {
			Thread.sleep(5000);
		}
		return null;
	}

	class ClustererEval implements Callable<Void> {

		final Clusterer clusterer;

		public ClustererEval(Clusterer clusterer) throws Exception {
			super();
			this.clusterer = clusterer;
		}

		public Void call() throws Exception {

			// if(clusterer.numberOfClusters())
//			if (clusterer instanceof SimpleKMeans){
				clusterer.buildClusterer(noClassSet);
//			} else {
//				clusterer.buildClusterer(trainingInsts);
//			}

			ClusterEvaluation eval = new ClusterEvaluation();
			eval.setClusterer(clusterer);
			eval.evaluateClusterer(trainingInsts);
			Writer wr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, clusterer.getClass()
									.getName(), "eval.txt")).getChannel(),
					"US-ASCII");
			try {
				wr.append("Stats\n====================\n")
						.append(eval.clusterResultsToString())
						.append("\n\nClass to Cluster\n====================\n")
						.append(Arrays.toString(eval.getClassesToClusters()))
						.append("\n\nLast Cluster Assignments\n====================\n")
						.append(Arrays.toString(eval.getClusterAssignments()))
						.append("\n\nLog Likelihood\n====================\n")
						.append(Double.toString(eval.getLogLikelihood()));
			} finally {
				wr.flush();
				wr.close();
			}
			return null;
		}

	}
}
