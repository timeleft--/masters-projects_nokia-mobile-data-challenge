package uwaterloo.mdc.visualize;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.stat.Frequency;
import org.apache.http.impl.cookie.DateUtils;
import org.math.plot.Plot2DPanel;
import org.math.plot.plots.ScatterPlot;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.util.StringUtils;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

public class PlaceLabelSeriesPlot extends JFrame {
	private static final boolean WEKA = true;
	private static boolean validate = true;
	private static boolean guess = false;
	private static boolean show = false;
	private static boolean save = false;
	// private static String classifierAndFeatSelector =
	// "weka.classifiers.trees.J48\\weka.attributeSelection.GainRatioAttributeEval";
	// "weka.classifiers.trees.RandomForest\\weka.attributeSelection.GainRatioAttributeEval";
	private static String inBase = "C:\\mdc-datasets\\weka\\"; // validation_sample-noweight_cascade-base_max1.5";
	// + classifierAndFeatSelector; //
	// \v0_u0_n1_ALL_feat-selected_instid-time_prediction.csv"
	private static String outBase = "C:\\mdc-datasets\\weka\\results";
	// "visualization\\place-label_time-series_validation_sample-noweight_cascade-base_max1.5\\";
	// + classifierAndFeatSelector;
	private static String FILENAME_SUFFIX = "ALL_feat-selected_instid-time_prediction.csv";
	private static String instIdPlaceIdMapsPath = "C:\\mdc-datasets\\weka\\segmented_user_sample-noweight\\";
	private static String instIdPlaceIdMapsFname = "_instid-placeid_map.properties";
	private static FilenameFilter fnFilter = new FilenameFilter() {

		@Override
		public boolean accept(File dir, String name) {
			boolean result = name.endsWith(FILENAME_SUFFIX);
			if (name.contains("_l")) {
				result &= name.contains("_l3");
			}
			return result;
		}
	};
	private static double[][] visitsConfusionMatrix;
	private static double[][] placesConfusionMatrix;
	private static Properties instIdPlaceId;
	static TreeMap<String, Frequency> placePredictions = new TreeMap<String, Frequency>();
	static SimpleDateFormat reverseDate = new SimpleDateFormat("yyyyMMddHHmm");
	private static double superiorityRatio = 1.1;

	public PlaceLabelSeriesPlot(String userId, String pfx) {
		setTitle(userId + pfx
				+ " - Close one window and all windows will get closed!");
		setSize(2100, 1400);
		setLocationRelativeTo(null);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
	}

	public static void main(String[] args) throws InterruptedException,
			IOException {
		Config.placeLabels = new Properties();
		FileInputStream placeLabelsIn = FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE));
		Config.placeLabels.load(placeLabelsIn);
		placeLabelsIn.close();

		if (WEKA) {
			doWeka();
		} else {
			inBase = "C:\\mdc-datasets\\svmlight\\output\\ALL";
			doSvmLight();
		}

	}

	static void writeConfusionMatrix(double[][] confusionMatrix, Writer evalWr)
			throws IOException {
		double[] precision = new double[11];
		double[] recall = new double[11];
		double avgPrec = 0.0;
		double avgRecall = 0.0;
		for (int i = 0; i < 11; ++i) {
			double numerator = confusionMatrix[i][i];
			double precDenim = 0.0;
			double recDenim = 0.0;
			for (int j = 0; j < 11; ++j) {
				precDenim += confusionMatrix[j][i];
				recDenim += confusionMatrix[i][j];
			}
			precision[i] = numerator / precDenim;
			recall[i] = numerator / recDenim;
			avgPrec += precision[i];
			avgRecall += recall[i];
		}
		evalWr.append(
				"Precision: " + Arrays.toString(precision) + " Average: "
						+ (avgPrec / 11) + "\n").append(
				"Recall: " + Arrays.toString(recall) + " Average: "
						+ (avgRecall / 11) + "\n");

		evalWr.append("Confusion Matrix:\n").append(
				"label\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\n");

		for (int i = 0; i < confusionMatrix.length; ++i) {
			evalWr.append(Integer.toString(i));
			long rowTotal = 0;

			for (int j = 0; j < Config.LABELS_SINGLES.length; ++j) {
				double cnt = confusionMatrix[i][j];
				rowTotal += cnt;
				evalWr.append('\t').append(Long.toString(Math.round(cnt)));
			}

			evalWr.append('\t').append(Long.toString(rowTotal)).append('\n');
		}

	}

	private static void writeFreqMap(String userId) throws IOException {
		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(userId + "_placeid-predictions.csv"))
						.getChannel(), Config.OUT_CHARSET);
		try {
			Random rand = new Random(System.currentTimeMillis());
			wr.append("placeid\tL0\tL1\tL2\tL3\tL4\tL5\tL6\tL7\tL8\tL9\tL10\tTotalCount\tSelected\tActual\n");
			TreeMap<String, Frequency> freqMap = placePredictions;
			for (String placeid : freqMap.keySet()) {
				wr.append(placeid);
				Frequency freq = freqMap.get(placeid);
				int total = 0;
				double maxPct = 1E-6;
				LinkedList<String> selLabels = new LinkedList<String>();
				for (String label : Config.LABELS_SINGLES) {
					double pct = freq.getPct(Double.parseDouble(label));
					wr.append('\t').append(Double.toString(pct));
					total += freq.getCount(label);

					double ratio = pct / maxPct;
					if (ratio >= 1 / superiorityRatio) {
						if (ratio > superiorityRatio) {
							selLabels.clear();
						}
						selLabels.add(label);
						if (ratio > 1) {
							maxPct = pct;
						}
					}
				}
				String actualLabel = Config.placeLabels.getProperty(placeid,
						"0");
				if (selLabels.size() == 0) {
					wr.append("No selected label!");
					continue;
				}
				String label = selLabels.get(rand.nextInt(selLabels.size()));
				wr.append("\t" + total).append("\t" + label)
						.append("\t" + actualLabel).append('\n');

				++placesConfusionMatrix[Integer.parseInt(actualLabel)][Integer
						.parseInt(label)];
			}

		} finally {
			wr.flush();
			wr.close();
		}
	}

	private static double guess(TreeMap<Double, Double> actual, int guess) {
		if (guess == 0) {
			Random rand = new Random(System.currentTimeMillis());
			guess = (rand.nextBoolean() ? 1 : 3);
		}
		double result = 0;
		for (Double truLabel : actual.values()) {
			if (truLabel == guess) {
				++result;
			}
		}
		return result;
	}

	private static long appendAccuracy(Writer accuracyWr, Frequency accuracy,
			String userId) throws IOException {
		long totalCnt = accuracy.getCount(Boolean.TRUE)
				+ accuracy.getCount(Boolean.FALSE);
		accuracyWr.append(userId + "\t" + accuracy.getPct(Boolean.TRUE) + "\t"
				+ accuracy.getPct(Boolean.FALSE) + "\t" + totalCnt + "\n");
		return totalCnt;
	}

	private static Frequency plotAndCalcAccuracy(String currUserId, String pfx,
			TreeMap<Double, Double> predicted, TreeMap<Double, Double> actual,
			LinkedList<String> instIds, String outPath) throws IOException,
			InterruptedException {
		Frequency accuracy = new Frequency();

		// Allign the first data point to plot to a monday 5 or 6 AM (TZ
		// unknown)
		double userStart = predicted.keySet().iterator().next();
		String timeZoneStr = Config.DEFAULT_TIME_ZONE;
		int timeZoneOffset = Integer.parseInt(timeZoneStr);

		char timeZonePlusMinus = '+';
		if (timeZoneStr.charAt(0) == '+') { // it's offset
			timeZonePlusMinus = '-';
		}
		// Offset in hours (from seconds)
		int hrs = Math.abs(timeZoneOffset / 3600);

		TimeZone timeZoneOfRecord = TimeZone.getTimeZone("GMT"
				+ timeZonePlusMinus + hrs);
		Calendar calendar = new GregorianCalendar();
		calendar.setTimeZone(timeZoneOfRecord);

		// Minus because the offset is from to the TZ to GMT
		calendar.setTimeInMillis((Math.round(userStart) - timeZoneOffset) * 1000);

		int tod = calendar.get(Calendar.HOUR_OF_DAY);
		// we want day of week to be relative to monday and starting from 0
		int dow = (calendar.get(Calendar.DAY_OF_WEEK) - 2) % 6;
		// timeSinceMondayMorning: in ten minute periods
		int tsmm = ((tod - 6) + (dow * 24)) * 6; // 6 ten minues every hour
		int arrLen = predicted.size() + (tsmm);
		double[][] predictedArr = new double[arrLen][2];
		double[][] actualArr = new double[arrLen][2];
		String[] instIdArr = new String[arrLen];
		for (int m = 0; m < (tsmm); ++m) {
			predictedArr[m][0] = m * Config.TIME_SECONDS_IN_10MINS;
			actualArr[m][0] = predictedArr[m][0];
			predictedArr[m][1] = 0;
			actualArr[m][1] = 0;
		}
		userStart -= tsmm * Config.TIME_SECONDS_IN_10MINS;
		int i = 0;
		for (Double time : predicted.keySet()) {
			predictedArr[i][0] = time - userStart;
			predictedArr[i][1] = predicted.get(time);
			instIdArr[i] = instIds.get(i);

			if (validate) {
				actualArr[i][0] = predictedArr[i][0];
				actualArr[i][1] = actual.get(time);

				accuracy.addValue(actualArr[i][1] == predictedArr[i][1]);
				// && actualArr[i][1] != 0);

				++visitsConfusionMatrix[(int) actualArr[i][1]][(int) predictedArr[i][1]];
				if (WEKA) {
					String placeId = instIdPlaceId.getProperty(instIdArr[i]
							+ ".0");
					Frequency placeFreq = placePredictions.get(placeId);
					if (placeFreq == null) {
						placeFreq = new Frequency();
						placePredictions.put(placeId, placeFreq);
					}
					placeFreq.addValue(predictedArr[i][1]);
				}
			}
			++i;
		}

		if (!show && !save) {
			return accuracy;
		}

		double numWeeks = Math.ceil(predictedArr.length
				/ (Config.TIME_10MINS_IN_WEEK));
		for (int w = 0; w < numWeeks; ++w) {
			int rangeStart = (int) Math
					.round(((w) * Config.TIME_10MINS_IN_WEEK));
			int rangeEnd = (int) Math
					.round(((w + 1) * Config.TIME_10MINS_IN_WEEK));
			if (rangeEnd > predictedArr.length) {
				rangeEnd = predictedArr.length;
			}
			final PlaceLabelSeriesPlot plotWindow = new PlaceLabelSeriesPlot(
					currUserId, "Week " + (w + 1) + " - " + pfx);

			double[][] predictedArrSlice = Arrays.copyOfRange(predictedArr,
					rangeStart, rangeEnd);

			Plot2DPanel plotPannel = new Plot2DPanel(new double[] { 0, 0 },
					new double[] { Config.TIME_SECONDS_IN_WEEK,
							// predictedArrSlice[predictedArrSlice.length -
							// 1][0],
							8 }, new String[] { "LIN", "LIN" },
					new String[] { "Time in seconds since first visit",
							"Place label" });
			plotPannel.setBackground(Color.WHITE);

			plotWindow.add(plotPannel);

			if (validate) {
				plotPannel.addScatterPlot("Actual", Color.RED,
						Arrays.copyOfRange(actualArr, rangeStart, rangeEnd));
				// actualPlot.setTags(instIdArr);
			}

			plotPannel.addScatterPlot("Predicted", Color.BLUE,
					predictedArrSlice);
			ScatterPlot predictedPlot = ((ScatterPlot) plotPannel.getPlot(1));
			predictedPlot.setTags(Arrays.copyOfRange(instIdArr, rangeStart,
					rangeEnd));

			if (show) {
				SwingUtilities.invokeLater(new Runnable() {
					@Override
					public void run() {
						plotWindow.setVisible(true);

					}
				});
			}
			if (save) {
				if (!show) {
					// Just so that we can call paint
					plotWindow.setVisible(true);
					plotWindow.setDefaultCloseOperation(DISPOSE_ON_CLOSE);

					// Give the slow component time to render!
					Thread.sleep(500);
				}
				Image image = plotPannel.createImage(plotPannel.getWidth(),
						plotPannel.getHeight());
				plotPannel.paint(image.getGraphics());
				image = new ImageIcon(image).getImage();

				BufferedImage bufferedImage = new BufferedImage(
						image.getWidth(null), image.getHeight(null),
						BufferedImage.TYPE_INT_RGB);
				Graphics g = bufferedImage.createGraphics();
				g.drawImage(image, 0, 0, Color.WHITE, null);
				g.dispose();

				OutputStream os = FileUtils.openOutputStream(FileUtils.getFile(
						outPath, currUserId, "week" + (w + 1) + "_" + pfx
								+ ".png"));
				try {
					ImageIO.write((RenderedImage) bufferedImage, "PNG", os);
				} finally {
					os.flush();
					os.close();
				}

				if (!show) {
					plotWindow.dispose();
				}
			}
		}
		return accuracy;
	}

	private static void doWeka() throws IOException {
		FileFilter dirFilter = new FileFilter() {

			@Override
			public boolean accept(File arg0) {
				return arg0.isDirectory();
			}
		};

		FileFilter validationFilter = new FileFilter() {
			@Override
			public boolean accept(File arg0) {
				return arg0.getName().startsWith(
						"validation_full-no-weight_bayesnet")// validation_sample-noweight_cascade-base_ties
						&& arg0.isDirectory();
			}
		};
		for (File validationDir : FileUtils.getFile(inBase).listFiles(
				validationFilter)) {
			String validationName = reverseDate.format(new Date(validationDir
					.lastModified())) + "_" + validationDir.getName();
			for (File classifierDir : validationDir.listFiles(dirFilter)) {
				for (File featSelDir : classifierDir.listFiles(dirFilter)) {

					String outPath = FilenameUtils.concat(
							FilenameUtils.concat(outBase,
									classifierDir.getName()),
							featSelDir.getName());
					Writer accuracyWr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, validationName,
											"accuracy.csv")).getChannel(),
							Config.OUT_CHARSET);
					double totalCount = 0;
					double trueCount = 0;
					// double mereGuess[] = {0,0,0};
					visitsConfusionMatrix = new double[11][11];
					placesConfusionMatrix = new double[11][11];
					TreeMap<Double, Double> predicted = new TreeMap<Double, Double>();
					TreeMap<Double, Double> actual = new TreeMap<Double, Double>();
					LinkedList<String> instIds = new LinkedList<String>();

					try {
						accuracyWr.append("User\tTRUE\tFALSE\tTotalCount\n");
						int fileIx = 0;
						for (File seriesFile : featSelDir.listFiles(fnFilter)) {
							try {
								if (fileIx == Config.NUM_USERS_TO_PROCESS) {
									break;
								}

								predicted.clear();
								actual.clear();
								instIds.clear();

								String pfx = seriesFile.getParentFile()
										.getParentFile().getName()
										+ "/"
										+ seriesFile.getParentFile().getName()
										+ "/"
										+ seriesFile.getName().substring(
												0,
												seriesFile.getName().indexOf(
														FILENAME_SUFFIX));

								CSVLoader csvLoad = new CSVLoader();
								csvLoad.setSource(seriesFile);
								// Instances struct = csvLoad.getStructure();
								Instances data = csvLoad.getDataSet();

								String currUserId = null;
								Enumeration instEnum = data
										.enumerateInstances();
								while (instEnum.hasMoreElements()) {
									Instance inst = (Instance) instEnum
											.nextElement();
									String instUserId = StringUtils
											.numberToId((int) Math.round(inst
													.value(0)));
									if (!instUserId.equals(currUserId)) {
										if (predicted.size() > 0) {
											System.err
													.println("Two users in the same timeline file");
											Frequency accuracy = plotAndCalcAccuracy(
													currUserId,
													pfx,
													predicted,
													actual,
													instIds,
													FilenameUtils
															.concat(outPath,
																	reverseDate
																			.format(new Date(
																					validationDir
																							.lastModified()))
																			+ "_"
																			+ validationDir
																					.getName()));
											// if(guess){
											// mereGuess[0] += guess(actual,0);
											// mereGuess[1] += guess(actual,1);
											// mereGuess[2] += guess(actual,3);
											// }
											long userCount = appendAccuracy(
													accuracyWr, accuracy,
													currUserId);
											totalCount += userCount;
											trueCount += userCount
													* accuracy
															.getPct(Boolean.TRUE);

											predicted.clear();
											actual.clear();
											instIds.clear();

											writeFreqMap(currUserId);
											placePredictions.clear();
										}
										instIdPlaceId = new Properties();
										FileInputStream in = FileUtils
												.openInputStream(FileUtils.getFile(FilenameUtils
														.concat(instIdPlaceIdMapsPath,
																instUserId
																		+ instIdPlaceIdMapsFname)));
										instIdPlaceId.load(in);
										in.close();
									}

									currUserId = instUserId;
									String instId = Long.toString(Math
											.round(inst.value(1)));

									Double startTime = inst.value(2);
									Double endTime = inst.value(3);
									Double instPredicted = inst.value(4);
									Double instActual = inst.value(5);

									for (double time = startTime; time <= endTime; time += Config.TIME_SECONDS_IN_10MINS) {
										predicted.put(time, instPredicted);
										if (validate) {
											actual.put(time, instActual);
										}
										instIds.add(instId);
									}

								}

								Frequency accuracy = plotAndCalcAccuracy(
										currUserId, pfx, predicted, actual,
										instIds, FilenameUtils.concat(outPath,
												validationName));
								long userCount = appendAccuracy(accuracyWr,
										accuracy, currUserId);
								// if(guess){
								// mereGuess[0] += guess(actual,0);
								// mereGuess[1] += guess(actual,1);
								// mereGuess[2] += guess(actual,3);
								// }
								totalCount += userCount;
								trueCount += userCount
										* accuracy.getPct(Boolean.TRUE);
								++fileIx;

								writeFreqMap(currUserId);
								placePredictions.clear();
							} catch (Exception ignored) {
								System.err.println(new Date()
										+ seriesFile.getAbsolutePath());
								ignored.printStackTrace();
							}
						}

					} finally {
						accuracyWr.flush();
						accuracyWr.close();
					}

					if (predicted.size() == 0) {
						continue;
						// System.out.println("Deleting "+ outPath);
						// FileUtils.deleteDirectory(FileUtils.getFile(outPath));
					} else {
						System.out.println("Keeping " + outPath);

					}

					// FileUtils.writeStringToFile(
					// FileUtils.getFile(outPath, "true-positive-rate.csv"),
					// trueCount + " / " + totalCount + " = "
					// + (trueCount * 1.0 / totalCount) + "\nGuess: " +
					// Arrays.toString(mereGuess));
					Writer evalWr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, validationName,
											"visits-eval.csv")).getChannel(),
							Config.OUT_CHARSET);
					try {
						evalWr.append(trueCount + " / " + totalCount + " = "
								+ (trueCount * 1.0 / totalCount) + "\n");

						writeConfusionMatrix(visitsConfusionMatrix, evalWr);

					} finally {
						evalWr.flush();
						evalWr.close();
					}
					evalWr = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, validationName,
											"places-eval.csv")).getChannel(),
							Config.OUT_CHARSET);
					try {

						writeConfusionMatrix(placesConfusionMatrix, evalWr);

					} finally {
						evalWr.flush();
						evalWr.close();
					}

				}
			}
		}
	}

	static class tFilter implements FilenameFilter {

		final int t;

		public tFilter(int t) {
			this.t = t;
		}

		@Override
		public boolean accept(File dir, String name) {

			return name.endsWith(t + "_predictions.txt");

		}
	}

	static void doSvmLight() throws IOException {
		FileFilter validationFilter = new FileFilter() {
			@Override
			public boolean accept(File arg0) {
				return arg0.getName().startsWith("v")// validation_sample-noweight_cascade-base_ties
						&& arg0.isDirectory();
			}
		};

		for (int t = 0; t < 1; ++t) {
			File inDir =  FileUtils.getFile(inBase);
			String validationName = reverseDate.format(new Date(inDir
					.lastModified())) + "_" + inDir.getName();

			String outPath = FilenameUtils.concat(
					FilenameUtils.concat(outBase, "svmlight"), "t" + t);
			Writer accuracyWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, validationName,
									"accuracy.csv")).getChannel(),
					Config.OUT_CHARSET);
			double totalCount = 0;
			double trueCount = 0;
			// double mereGuess[] = {0,0,0};
			visitsConfusionMatrix = new double[11][11];
			placesConfusionMatrix = new double[11][11];
			TreeMap<Double, Double> predicted = new TreeMap<Double, Double>();
			TreeMap<Double, Double> actual = new TreeMap<Double, Double>();
			LinkedList<String> instIds = new LinkedList<String>();

			try {
				accuracyWr.append("User\tTRUE\tFALSE\tTotalCount\n");
				int fileIx = 0;
				for (File validationDir :inDir.listFiles(
						validationFilter)) {
					for (File seriesFile : validationDir.listFiles(new tFilter(
							t))) {
						try {
							if (fileIx == Config.NUM_USERS_TO_PROCESS) {
								break;
							}

							predicted.clear();
							actual.clear();
							instIds.clear();

							String pfx = "t" + t;

							// instIdPlaceId = new Properties();
							// FileInputStream in = FileUtils
							// .openInputStream(FileUtils.getFile(FilenameUtils
							// .concat(instIdPlaceIdMapsPath,
							// instUserId
							// + instIdPlaceIdMapsFname)));
							// instIdPlaceId.load(in);
							// in.close();

							BufferedReader pRd = new BufferedReader(
									Channels.newReader(FileUtils
											.openInputStream(seriesFile)
											.getChannel(), "US-ASCII"));
							BufferedReader aRd = new BufferedReader(
									Channels.newReader(
											FileUtils
													.openInputStream(
															FileUtils
																	.getFile(
																			"C:\\mdc-datasets\\svmlight\\input\\ALL",
																			validationDir
																					.getName(),
																			"validate.csv"))
													.getChannel(), "US-ASCII"));
							int instIdInt = 1;
							String pLine, aLine;
							while ((pLine = pRd.readLine()) != null) {
								aLine = aRd.readLine();

								Double instPredicted = Double.valueOf(pLine
										.substring(0, pLine.indexOf(' ')));
								if (instPredicted == 11) {
									instPredicted = 0.0;
								}
								Double instActual = Double.valueOf(aLine
										.substring(0, aLine.indexOf(' ')));
								if (instActual == 11) {
									instActual = 0.0;
								}
								String instId = Long.toString(instIdInt);
								double startTime = instIdInt * 600;
								double endTime = startTime + 601;

								for (double time = startTime; time <= endTime; time += Config.TIME_SECONDS_IN_10MINS) {
									predicted.put(time, instPredicted);
									if (validate) {
										actual.put(time, instActual);
									}
									instIds.add(instId);
								}
								++instIdInt;
							}

							Frequency accuracy = plotAndCalcAccuracy(
									validationDir.getName(), pfx, predicted,
									actual, instIds, FilenameUtils.concat(
											outPath, validationName));
							long userCount = appendAccuracy(accuracyWr,
									accuracy, validationDir.getName());
							// if(guess){
							// mereGuess[0] += guess(actual,0);
							// mereGuess[1] += guess(actual,1);
							// mereGuess[2] += guess(actual,3);
							// }
							totalCount += userCount;
							trueCount += userCount
									* accuracy.getPct(Boolean.TRUE);
							++fileIx;

							placePredictions.clear();
						} catch (Exception ignored) {
							System.err.println(new Date()
									+ seriesFile.getAbsolutePath());
							ignored.printStackTrace();
						}
					}
				}

			} finally {
				accuracyWr.flush();
				accuracyWr.close();
			}

			if (predicted.size() == 0) {
				continue;
				// System.out.println("Deleting "+ outPath);
				// FileUtils.deleteDirectory(FileUtils.getFile(outPath));
			} else {
				System.out.println("Keeping " + outPath);

			}

			// FileUtils.writeStringToFile(
			// FileUtils.getFile(outPath, "true-positive-rate.csv"),
			// trueCount + " / " + totalCount + " = "
			// + (trueCount * 1.0 / totalCount) + "\nGuess: " +
			// Arrays.toString(mereGuess));
			Writer evalWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, validationName,
									"visits-eval.csv")).getChannel(),
					Config.OUT_CHARSET);
			try {
				evalWr.append(trueCount + " / " + totalCount + " = "
						+ (trueCount * 1.0 / totalCount) + "\n");

				writeConfusionMatrix(visitsConfusionMatrix, evalWr);

			} finally {
				evalWr.flush();
				evalWr.close();
			}
			evalWr = Channels.newWriter(
					FileUtils.openOutputStream(
							FileUtils.getFile(outPath, validationName,
									"places-eval.csv")).getChannel(),
					Config.OUT_CHARSET);
			try {

				writeConfusionMatrix(placesConfusionMatrix, evalWr);

			} finally {
				evalWr.flush();
				evalWr.close();
			}
		}
	}
}
