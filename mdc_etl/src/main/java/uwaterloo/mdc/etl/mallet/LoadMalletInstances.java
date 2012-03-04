package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.types.InstanceList;
import cc.mallet.util.CharSequenceLexer;

public class LoadMalletInstances {

	private String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";
	private InstanceList training;
	private InstanceList testing;

	public LoadMalletInstances() {

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

		LoadMalletInstances app = new LoadMalletInstances();
		app.loadInstances();
	}

	public void loadInstances() {
		Pipe pipe = new SerialPipes(new Pipe[] {
				new Input2CharSequence(),
				new CharSequence2TokenSequence(
						CharSequenceLexer.LEX_NONWHITESPACE_TOGETHER) });
		training = new InstanceList(pipe);
		testing = new InstanceList(pipe);

		File inputDir = FileUtils.getFile(inputPath);
		File[] userDirs = inputDir.listFiles();
		for (int i = 0; i < userDirs.length; ++i) {
			VisitsIterator visitsIter = new VisitsIterator(userDirs[i]);
			if (i < Config.NUMBER_TESTING_USERS) {
				testing.addThruPipe(visitsIter);
			} else {
				training.addThruPipe(visitsIter);
			}
		}
	}

	public InstanceList getTraining() {
		return training;
	}

	public InstanceList getTesting() {
		return testing;
	}
}
