package uwaterloo.mdc.mallet;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.mallet.VisitsIterator;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.topics.HierarchicalLDA;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;
import cc.mallet.util.CharSequenceLexer;
import cc.mallet.util.Randoms;

public class AuthorTopicAnalysis implements Callable<Void> {

	private String outputPath = "C:\\mdc-datasets\\mallet\\out";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";
	private InstanceList training;
	private InstanceList validation;

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

		AuthorTopicAnalysis app = new AuthorTopicAnalysis();
		app.call();
	}

	public Void call() throws Exception {
		// app.hierarchicalLDA();
		this.perUserHLDA();

		return null;
	}

	public void perUserHLDA() throws Exception {
		Pipe pipe = new SerialPipes(new Pipe[] {
				new Input2CharSequence(),
				new CharSequence2TokenSequence(
						CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER),
				new TokenSequence2FeatureSequence() });

		for (int v = 0; v < Config.VALIDATION_FOLDS; ++v) {
			Writer likelihoodWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outputPath, "v" + v,
									"hlda-likelihood.txt")).getChannel(),
					Config.OUT_CHARSET);
			likelihoodWr.append("userid\tlikelihood\n");
			try {
				File inputDir = FileUtils.getFile(inputPath);
				for (File userDir : inputDir.listFiles()) {

					training = new InstanceList(pipe);
					validation = new InstanceList(pipe);

					VisitsIterator visitsIter = new VisitsIterator(userDir);
					int i = 0;
					while (visitsIter.hasNext()) {
						Instance inst = visitsIter.next();
						if (i == v) {
							validation.addThruPipe(inst);
						} else {
							training.addThruPipe(inst);
						}

						++i;
						if (i == Config.VALIDATION_FOLDS) {
							i = 0;
						}
					}

					if (training.size() == 0 || validation.size() == 0) {
						System.err.println("Not enough data for user: "
								+ userDir.getAbsolutePath());
						continue;
					}

					Randoms rand = new Randoms();
					rand.setSeed(System.currentTimeMillis());

					HierarchicalLDA hlda = new HierarchicalLDA();
					// testing is not used by the method below.. no need for a
					// validation
					hlda.initialize(training, validation, 3, rand);
					hlda.estimate(250);

					// Free up some memory
					training = null;

					Writer stateWr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outputPath, "v" + v,
											userDir.getName()
													+ "_hlda-state.txt"))
									.getChannel(), Config.OUT_CHARSET);
					try {
						hlda.printState(new PrintWriter(stateWr));
					} finally {
						stateWr.flush();
						stateWr.close();
					}

					OutputStream nodesOut = FileUtils
							.openOutputStream(FileUtils.getFile(outputPath, "v"
									+ v, userDir.getName() + "_hlda-nodes.txt"));

					PrintStream sysout = System.out;
					System.setOut(new PrintStream(nodesOut));
					try {
						hlda.printNodes();
					} finally {
						nodesOut.flush();
						nodesOut.close();
						System.setOut(sysout);
					}
					double empiricalLikelihood = hlda.empiricalLikelihood(100,
							validation);
					likelihoodWr.append(userDir.getName()).append('\t')
							.append(Double.toString(empiricalLikelihood))
							.append('\n');

				}
			} finally {
				likelihoodWr.flush();
				likelihoodWr.close();
			}
		}
	}
	// public void hierarchicalLDA() throws FileNotFoundException, IOException {
	//
	// Pipe pipe = new SerialPipes(new Pipe[] {
	// new Input2CharSequence(),
	// new CharSequence2TokenSequence(
	// CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER),
	// new TokenSequence2FeatureSequence() });
	// training = new InstanceList(pipe);
	// validation = new InstanceList(pipe);
	//
	// File inputDir = FileUtils.getFile(inputPath);
	// File[] userDirs = inputDir.listFiles();
	// for (int i = Config.NUMBER_TESTING_USERS; i < Config.NUMBER_TESTING_USERS
	// + 10/*
	// * userDirs
	// * .
	// * length
	// */; ++i) {
	// VisitsIterator visitsIter = new VisitsIterator(userDirs[i]);
	// training.addThruPipe(visitsIter);
	// }
	//
	// Randoms rand = new Randoms();
	// rand.setSeed(System.currentTimeMillis());
	//
	// HierarchicalLDA hlda = new HierarchicalLDA();
	// // testing is not used by the method below.. no need for a validation
	// hlda.initialize(training, validation, 3, rand);
	// hlda.estimate(250);
	//
	// File stateFile = FileUtils.getFile(outputPath, "hlda_state.txt");
	// hlda.printState(new PrintWriter(stateFile));
	//
	// training = null;
	// for (int i = 0; i < Config.NUMBER_TESTING_USERS; ++i) {
	// VisitsIterator visitsIter = new VisitsIterator(userDirs[i]);
	// validation.addThruPipe(visitsIter);
	// }
	//
	// double empiricalLikelihood = hlda.empiricalLikelihood(100, validation);
	// System.out.println("Empirical likelihood: " + empiricalLikelihood);
	//
	// }

}
