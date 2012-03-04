package uwaterloo.mdc.mallet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.mallet.VisitsIterator;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.topics.HierarchicalLDA;
import cc.mallet.types.InstanceList;
import cc.mallet.util.CharSequenceLexer;
import cc.mallet.util.Randoms;

public class AuthorTopicAnalysis {

	private String outputPath = "C:\\mdc-datasets\\mallet\\lda";
	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";
	private InstanceList training;
	private InstanceList testing;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

		AuthorTopicAnalysis app = new AuthorTopicAnalysis();
		app.hierarchicalLDA();

	}

	public void hierarchicalLDA() throws FileNotFoundException, IOException {

		Pipe pipe = new SerialPipes(new Pipe[] {
				new Input2CharSequence(),
				new CharSequence2TokenSequence(
						CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER) });
		training = new InstanceList(pipe);
		testing = new InstanceList(pipe);

		File inputDir = FileUtils.getFile(inputPath);
		File[] userDirs = inputDir.listFiles();
		for (int i = Config.NUMBER_TESTING_USERS; i < userDirs.length; ++i) {
			VisitsIterator visitsIter = new VisitsIterator(userDirs[i]);
			training.addThruPipe(visitsIter);
		}

		Randoms rand = new Randoms();
		rand.setSeed(System.currentTimeMillis());

		HierarchicalLDA hlda = new HierarchicalLDA();
		// testing is not used by the method below.. no need for a validation
		hlda.initialize(training, testing, 3, rand);
		hlda.estimate(250);

		File stateFile = FileUtils.getFile(outputPath, "hlda_state.txt");
		hlda.printState(new PrintWriter(stateFile));

		training = null;
		for (int i = 0; i < Config.NUMBER_TESTING_USERS; ++i) {
			VisitsIterator visitsIter = new VisitsIterator(userDirs[i]);
			testing.addThruPipe(visitsIter);
		}
		
		double empiricalLikelihood = hlda.empiricalLikelihood(1000,
				testing);
		System.out.println("Empirical likelihood: " + empiricalLikelihood);

	}

}
