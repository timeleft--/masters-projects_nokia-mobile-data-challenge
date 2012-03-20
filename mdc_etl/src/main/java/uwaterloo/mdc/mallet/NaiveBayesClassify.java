package uwaterloo.mdc.mallet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.mallet.VisitsIterator;
import cc.mallet.classify.Classification;
import cc.mallet.classify.NaiveBayes;
import cc.mallet.classify.NaiveBayesTrainer;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.FeatureSequence2FeatureVector;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.Target2Label;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;
import cc.mallet.types.LabelAlphabet;
import cc.mallet.types.Multinomial.Logged;
import cc.mallet.util.CharSequenceLexer;

public class NaiveBayesClassify implements Callable<Void> {

	private class FoldCallable implements Callable<Double> {

		Pipe pipe = new SerialPipes(new Pipe[] {
				new Target2Label(),
				new Input2CharSequence(),
				new CharSequence2TokenSequence(
						CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER),
				new TokenSequence2FeatureSequence(),
				new FeatureSequence2FeatureVector() });

		final int v;

		public FoldCallable(int fold) {
			this.v = fold;
		}

		@Override
		public Double call() throws Exception {
			double accuracy = 0;
			Frequency[] foldConfusionMatrix;

			foldConfusionMatrix = new Frequency[Config.LABELS_SINGLES.length];
			for (int i = 0; i < foldConfusionMatrix.length; ++i) {
				foldConfusionMatrix[i] = new Frequency();
			}
			int foldStart = v * Config.VALIDATION_FOLD_WIDTH;

			InstanceList validation = new InstanceList(pipe);
			NaiveBayesTrainer nbt = new NaiveBayesTrainer(pipe);
			NaiveBayes nbc = null;
			InstanceList training = new InstanceList(pipe);

			File[] inputDirs = FileUtils.getFile(inputPath).listFiles();

			for (int i = 0; i < inputDirs.length; ++i) {

				File userDir = inputDirs[i];
				VisitsIterator visitsIter = new VisitsIterator(userDir);
				training = new InstanceList(pipe);
				while (visitsIter.hasNext()) {
					Instance inst = visitsIter.next();
					if (i == (foldStart + inFoldTestIx)) {
						validation.addThruPipe(inst);
					} else {
						training.addThruPipe(inst);
					}
				}

				if (i != (foldStart + inFoldTestIx)) {

					if (training.size() == 0) {
						System.err.println("No training data from user: "
								+ userDir.getAbsolutePath());
						continue;
					}

					nbc = nbt.trainIncremental(training);
				}
				// free memory
				training = null;
				System.out.println((System.currentTimeMillis() - startTime)
						+ " (fold " + v + "): Done reading user: " + userDir.getName());
			}
			System.out.println((System.currentTimeMillis() - startTime)
					+ " (fold " + v + "): Finished training for fold: " + v);
			Writer classificationsWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v
									+ "_naive-bayes_classifications.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {

				// // Without incremental
				// NaiveBayesTrainer nbt = new NaiveBayesTrainer();
				// NaiveBayes nbc = nbt.train(training);

				classificationsWr
						.append("************** Seen Labels Distribuitions **************\n");
				LabelAlphabet labelAlpha = nbc.getLabelAlphabet();
				Logged[] p = nbc.getMultinomials();
				for (int i = 0; i < p.length; ++i) {
					// Bogus:
					// stateWr.append(labelAlpha.lookupLabel(i).toString()).append('\t')
					classificationsWr
							.append(labelAlpha.lookupObject(i).toString())
							.append('\t').append(p[i].toString(true))
							.append('\n');
				}
				classificationsWr
						.append("************** Classifications **************\n");
				if (validation.size() == 0) {
					System.err.println("Not validation data for fold: " + v);

				} else {
					int trueClassificationsCount = 0;
					for (Instance vInst : validation) {
						Classification vClass = nbc.classify(vInst);
						vClass.printRank(new PrintWriter(classificationsWr));
						if (vClass.bestLabelIsCorrect()) {
							++trueClassificationsCount;
						}
						int trueLabelCfMIx = Integer.parseInt(vInst.getTarget()
								.toString()) - 1;
						int bestLabelInt = Integer.parseInt(vClass
								.getLabeling().getBestLabel().toString());
						foldConfusionMatrix[trueLabelCfMIx]
								.addValue(bestLabelInt);
						synchronized (totalConfusionMatrix) {

							totalConfusionMatrix[trueLabelCfMIx]
									.addValue(bestLabelInt);
						}
					}
					accuracy = trueClassificationsCount * 1.0
							/ validation.size();
					synchronized (cvAccuracyWr) {

						cvAccuracyWr
								.append(Integer.toString(v))
								.append('\t')
								.append(Integer
										.toString(trueClassificationsCount))
								.append('\t')
								.append(Integer.toString(validation.size()))
								.append('\t').append(Double.toString(accuracy))
								.append('\n');
					}

				}
			} finally {
				classificationsWr.flush();
				classificationsWr.close();
			}
			Writer foldConfusionWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v
									+ "_naive-bayes_confusion-matrix.txt"))
							.getChannel(), Config.OUT_CHARSET);
			try {
				writeConfusionMatrix(foldConfusionWr, foldConfusionMatrix);
			} finally {
				foldConfusionWr.flush();
				foldConfusionWr.close();
			}
			System.out.println((System.currentTimeMillis() - startTime)
					+ " (fold " + v + "): Finished validation for fold: " + v);
			return accuracy;
		}

	}

	private String outputPath = "C:\\mdc-datasets\\mallet\\naive-bayes";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";

	private final Frequency[] totalConfusionMatrix;
	private long startTime = System.currentTimeMillis();

	private final int inFoldTestIx = new Random(System.currentTimeMillis())
			.nextInt(Config.VALIDATION_FOLD_WIDTH);

	private final Writer cvAccuracyWr;

	public NaiveBayesClassify() throws IOException {
		totalConfusionMatrix = new Frequency[Config.LABELS_SINGLES.length];
		for (int i = 0; i < totalConfusionMatrix.length; ++i) {
			totalConfusionMatrix[i] = new Frequency();
		}
		cvAccuracyWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath,
								"naive-bayes_accuracy-cv.txt")).getChannel(),
				Config.OUT_CHARSET);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));
		Config.quantizedFields = new Properties();
		Config.quantizedFields.load(FileUtils.openInputStream(FileUtils.getFile(Config.QUANTIZED_FIELDS_PROPERTIES)));
		
		// ImportIntoMallet importer = new ImportIntoMallet();
		// importer.createDocuments();

		NaiveBayesClassify app = new NaiveBayesClassify();
		app.call();
	}

	public Void call() throws Exception {
		this.acrossUsersClassify();

		return null;
	}

	public void acrossUsersClassify() throws Exception {

		SummaryStatistics accuracySummary = new SummaryStatistics();

		cvAccuracyWr.append("vFold").append('\t').append("trueN").append('\t')
				.append("totalN").append('\t').append("accuracy").append('\n');
		try {

			System.out.println(new Date().toString()
					+ " (total): Validating with every user number " + inFoldTestIx
					+ " within every " + Config.VALIDATION_FOLD_WIDTH
					+ " users");
			ExecutorService foldExecutors = Executors
					.newFixedThreadPool(Config.NUM_THREADS);
			ArrayList<Future<Double>> foldFutures = new ArrayList<Future<Double>>();
			int numClassifyTasks = 0;
			for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
				FoldCallable FoldCallable = new FoldCallable(v);
				foldFutures.add(foldExecutors.submit(FoldCallable));
				++numClassifyTasks;
			}
			for (int i = 0; i < numClassifyTasks; ++i) {
				accuracySummary.addValue(foldFutures.get(i).get());
			}
			foldExecutors.shutdown();
			while(!foldExecutors.isTerminated()){
				System.out.println((System.currentTimeMillis() - startTime)
						+ " (total): shutting down");
				Thread.sleep(1000);
			}
		} finally {
			cvAccuracyWr.flush();
			cvAccuracyWr.close();
		}
		FileUtils
				.writeStringToFile(FileUtils.getFile(outputPath,
						"naive-bayes_accuracy-summary.txt"), accuracySummary
						.toString());

		Writer totalConfusionWr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outputPath,
								"naive-bayes_confusion-matrix.txt"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			writeConfusionMatrix(totalConfusionWr, totalConfusionMatrix);
		} finally {
			totalConfusionWr.flush();
			totalConfusionWr.close();
		}

		System.out.println(new Date().toString() + " (total): Done in "
				+ (System.currentTimeMillis() - startTime) + " millis");
	}

	public static void writeConfusionMatrix(Writer foldConfusionWr,
			Frequency[] foldConfusionMatrix) throws IOException {
		foldConfusionWr.append("label\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ttotal\n");
		for (int i = 0; i < foldConfusionMatrix.length; ++i) {
			foldConfusionWr.append(Integer.toString(i+1));
			long totalCount = 0;
			for (int j = 1; j <= Config.LABELS_SINGLES.length; ++j) {
				long cnt = foldConfusionMatrix[i].getCount(j);
				totalCount += cnt;
				foldConfusionWr.append('\t').append(Long.toString(cnt));
			}
			foldConfusionWr.append('\t').append(Long.toString(totalCount))
					.append('\n');
		}
	}

}
