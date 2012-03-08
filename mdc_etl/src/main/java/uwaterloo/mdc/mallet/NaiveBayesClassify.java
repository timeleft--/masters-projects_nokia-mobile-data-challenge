package uwaterloo.mdc.mallet;

import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;

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

	private String outputPath = "C:\\mdc-datasets\\mallet\\naive-bayes";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

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
		Pipe pipe = new SerialPipes(new Pipe[] {
				new Target2Label(),
				new Input2CharSequence(),
				new CharSequence2TokenSequence(
						CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER),
				new TokenSequence2FeatureSequence(),
				new FeatureSequence2FeatureVector() });

		int inFoldTestIx = new Random().nextInt(Config.VALIDATION_FOLD_WIDTH);
		System.out.println("Validating with every user number " + inFoldTestIx
				+ " in a every " + Config.VALIDATION_FOLD_WIDTH + " users");
		for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
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
						System.err.println("Not enough data for user: "
								+ userDir.getAbsolutePath());
						continue;
					}

					nbc = nbt.trainIncremental(training);
				}
				// free memory
				training = null;
				System.out.println("Done reading user: " + userDir.getName());
			}
			System.out.println("Finished training for fold: " + v);
			Writer stateWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v,
									"naive-bayes_results.txt")).getChannel(),
					Config.OUT_CHARSET);
			try {

				// // Without incremental
				// NaiveBayesTrainer nbt = new NaiveBayesTrainer();
				// NaiveBayes nbc = nbt.train(training);

				stateWr.append("************** Seen Labels Distribuitions **************\n");
				LabelAlphabet labelAlpha = nbc.getLabelAlphabet();
				Logged[] p = nbc.getMultinomials();
				for (int i = 0; i < p.length; ++i) {
					// Bogus:
					// stateWr.append(labelAlpha.lookupLabel(i).toString()).append('\t')
					stateWr.append(labelAlpha.lookupObject(i).toString())
							.append('\t').append(p[i].toString(true))
							.append('\n');
				}
				stateWr.append("************** Classifications **************\n");

				if (validation.size() == 0) {
					System.err.println("Not validation data for fold: " + v);

				} else {
					int trueClass = 0;
					for (Instance vInst : validation) {
						Classification vClass = nbc.classify(vInst);
						vClass.printRank(new PrintWriter(stateWr));
						if (vClass.bestLabelIsCorrect()) {
							++trueClass;
						}
					}
					stateWr.append(
							"************** \"Accuracy\" **************\n")
							.append(trueClass
									+ "/"
									+ validation.size()
									+ "="
									+ Double.toString(trueClass * 1.0
											/ validation.size()));
				}
			} finally {
				stateWr.flush();
				stateWr.close();
			}
			System.out.println("Finished validation for fold: " + v);
		}
	}

}
