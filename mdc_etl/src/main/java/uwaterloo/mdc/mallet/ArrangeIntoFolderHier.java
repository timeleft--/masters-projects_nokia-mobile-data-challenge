package uwaterloo.mdc.mallet;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.StringUtils;
import uwaterloo.util.NotifyStream;

public class ArrangeIntoFolderHier {
	private static String inputPath = "C:\\mdc-datasets\\mallet\\segmented_user-time";

	private static final String OUTPUT_PATH = "c:\\mdc-datasets\\mallet-in\\";
	static class Worker implements Callable<Void> {
		
		private final File userDir;
		
		public Worker(File userDir) {
			this.userDir = userDir;
		}
		public Void call() throws Exception{
			String relName = userDir.getName();

			System.out.println((new Date())
					+ ": Reading user " + relName);

			Collection<File> microLocsFiles = FileUtils.listFiles(userDir,
					new String[] { "csv" }, true);
						Long prevStartTime = null;
			Long prevEndTime = null;
			String prevLabel = null;
			String instLabel = null;
			String placeID = null;
			double instId = 1.0;
				for (File microLocF : microLocsFiles) {
					if (!Config.MICROLOC_SPLITS_DOCS) {
						if (microLocF.getName().endsWith(
								"" + Config.TIMETRUSTED_WLAN)) {
							throw new AssertionError("Splitted!!");
						}
					}


					Reader microLocR = Channels.newReader(FileUtils
							.openInputStream(microLocF).getChannel(),
							Config.OUT_CHARSET);
					int chInt;
					int numTabs = 0;
					StringBuffer header = new StringBuffer();

					long currStartTime = Long.parseLong(StringUtils
							.removeLastNChars(microLocF.getParentFile()
									.getName(), 1));
					long currEndTime = Long.parseLong(StringUtils
							.removeLastNChars(microLocF.getName(), 5));
					while ((chInt = microLocR.read()) != -1) {
						if ((char) chInt == '\t') {
							if (numTabs == 0) {
								// inst name.. not useful in weka
							} else if (numTabs == 1) {
								if (prevStartTime != null
										&& prevStartTime.equals(currStartTime)) {
									// same visit, different micro loc.. nothing
									// to
									// do
									// System.out.println("dummy!");
								} else if (prevEndTime != null
										&& currStartTime - prevEndTime < Config.INTERVAL_LABEL_CARRY_OVER) {
									prevLabel = instLabel;
								} else {
									prevLabel = null;
								}
								placeID = header.toString();
								instLabel = Config.placeLabels
										.getProperty(placeID, "0");
							}
							header.setLength(0);
							++numTabs;
							if (numTabs == 2) {
								break;
							}
						} else {
							header.append((char) chInt);
						}
					}
					prevStartTime = currStartTime;
					prevEndTime = currEndTime;
					header = null;
					double visitLength = (currEndTime - currStartTime)
							/ Config.TIME_SECONDS_IN_10MINS * 1.0;
					File placeFile = FileUtils.getFile(OUTPUT_PATH,instLabel,placeID+".csv");
					Writer wr = Channels.newWriter(FileUtils.openOutputStream(placeFile,placeFile.exists()).getChannel(),"UTF-8");
					try{
						while ((chInt = microLocR.read()) != -1) {
							wr.write(chInt);
						}
						wr.write("\n");
					}finally{
						wr.flush();
						wr.close();
					}
				++instId;
			}
		System.out.println((new Date())
				+ ": Finished reading user " + relName);
		return null;
	
		}
	}

	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
//		PrintStream errOrig = System.err;
//		NotifyStream notifyStream = new NotifyStream(errOrig,
//				"ArrangeIntoFplderHierarchy");
//		try {
//			System.setErr(new PrintStream(notifyStream));

			Config.placeLabels = new Properties();
			Config.placeLabels.load(FileUtils.openInputStream(FileUtils
					.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));

			File dataDir = FileUtils.getFile(inputPath);

			for (File userDir : dataDir.listFiles()) {
				new Worker(userDir).call();
			}
			System.err.println(new Date() + ": Done in "
					+ (new Date()) + " millis");
//		} finally {
//			try {
//				notifyStream.close();
//			} catch (IOException ignored) {
//
//			}
//			System.setErr(errOrig);
//		}
	}

}
