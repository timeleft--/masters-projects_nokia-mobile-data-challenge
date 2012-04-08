package uwaterloo.mdc.weka;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import org.apache.commons.io.FileUtils;
import org.math.plot.Plot3DPanel;
import org.math.plot.plots.ScatterPlot;
import org.math.plot.utils.Array;

import uwaterloo.mdc.etl.Config;
import weka.attributeSelection.ASSearch;
import weka.attributeSelection.PrincipalComponents;
import weka.attributeSelection.Ranker;
import weka.classifiers.Classifier;
import weka.classifiers.meta.AttributeSelectedClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.AddID;
import weka.filters.unsupervised.attribute.Remove;

public class ClusterIn3DSpace extends JFrame {

	private static String inPath = "C:\\mdc-datasets\\weka\\segmented_user\\ALL";
	private static String outPath = "C:\\mdc-datasets\\weka\\data-viz\\pca";

	public ClusterIn3DSpace(String userId) {
		setTitle(userId + " - Close one window and all windows will get closed!");
		setSize(2100, 1400);
		setLocationRelativeTo(null);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Instances trainingSet = null;

		int fileIx = 0;
		for (File inFile : FileUtils.listFiles(FileUtils.getFile(inPath),
				new String[] { "arff" }, false)) {
			if (fileIx == Config.NUM_USERS_TO_PROCESS) {
				break;
			}
			ArffLoader dataLoader = new ArffLoader();
			dataLoader.setFile(inFile);

			Instances dataStruct = dataLoader.getStructure();
			dataStruct.setClassIndex(dataStruct.numAttributes() - 1);

			if (fileIx == 0) {
				trainingSet = new Instances(dataStruct);
				
				// Generate the full legend
				trainingSet.setRelationName("lgd");
				for(int l=0;l<Config.LABELS_SINGLES.length; ++l){
				Instance toyInst = new Instance(dataStruct.numAttributes());
				toyInst.setDataset(trainingSet);
				trainingSet.add(toyInst);
				for(int i=0; i<4; ++i){
				toyInst.setValue(i, i);
				}
				toyInst.setClassValue(l);
				}
				writeOutAndPlot(trainingSet);
			
				trainingSet = new Instances(dataStruct);
			}

			Instance dataInst;
			while ((dataInst = dataLoader.getNextInstance(dataStruct)) != null) {
				trainingSet.add(dataInst);
			}

			if (Config.CLUSTER_IN3D_PER_USER) {
				writeOutAndPlot(transformAndCluster(trainingSet));
				trainingSet = new Instances(dataStruct);
			}

			++fileIx;
		}

		if (!Config.CLUSTER_IN3D_PER_USER) {
			writeOutAndPlot(transformAndCluster(trainingSet));
		}

	}

	public static Instances transformAndCluster(Instances trainingSet)
			throws Exception {
		Classifier baseClassifier = new J48();
		baseClassifier.buildClassifier(trainingSet);

		PrincipalComponents attrSelection = new PrincipalComponents();

		ASSearch attrSearch = new Ranker();
		AttributeSelectedClassifier asClassifier = new AttributeSelectedClassifier();
		asClassifier.setClassifier(baseClassifier);
		asClassifier.setEvaluator(attrSelection);
		asClassifier.setSearch(attrSearch);
		asClassifier.buildClassifier(trainingSet);

		if (Config.CLUSTER_IN3D_IN_TRANFORMED_SPACE) {
			trainingSet = attrSelection.transformedData(trainingSet);
			AddID addid = new AddID();
			addid.setAttributeName("ID");
			addid.setInputFormat(trainingSet);
			trainingSet = Filter.useFilter(trainingSet, addid);
		}

		Remove remClass = new Remove();
		remClass.setInputFormat(trainingSet);
		remClass.setAttributeIndices("last"); //
		Integer.toString(trainingSet.numAttributes());
		Instances noClassSet = Filter.useFilter(trainingSet, remClass);
		// Work around:
		if (noClassSet.attribute(noClassSet.numAttributes() - 1).name()
				.equals("label")) {
			noClassSet.setClassIndex(-1);
			noClassSet.deleteAttributeAt(noClassSet.numAttributes() - 1);
		}

		// Random rand = new Random(System.currentTimeMillis());
		// Clusterer clusterer = null;
		// for (int n = 4; n <= 11; ++n) {
		// double bestSqE = Double.MAX_VALUE;
		//
		// for (int i = 0; i < Config.CLUSTERCLASSIFY_NUM_KMEAN_RUNS; i++) {
		// Clusterer sk = new SimpleKMeans();
		// if (sk instanceof RandomizableClusterer) {
		// ((RandomizableClusterer) sk).setSeed(rand.nextInt());
		// }
		// // if (sk instanceof NumberOfClustersRequestable) {
		// ((SimpleKMeans) sk).setNumClusters(n);
		//
		// // sk.setDisplayStdDevs(true);
		// // sk.setDistanceFunction(new ManhattanDistance(noClassSet));
		// sk.buildClusterer(noClassSet);
		// if (sk instanceof SimpleKMeans) {
		//
		// if (((SimpleKMeans) sk).getSquaredError() < bestSqE) {
		// bestSqE = ((SimpleKMeans) sk).getSquaredError();
		// clusterer = sk;
		// }
		// } else {
		// clusterer = sk;
		// break;
		// }
		// }
		return trainingSet;
	}

	public static void writeOutAndPlot(Instances trainingSet)
			throws IOException {
		String userId = trainingSet.relationName().substring(0, 3);
		// Label,Cluster\tFirst3Attrs
		double attr1[][] = new double[Config.LABELS_SINGLES.length][trainingSet
				.numInstances()];
		double attr2[][] = new double[Config.LABELS_SINGLES.length][trainingSet
				.numInstances()];
		double attr3[][] = new double[Config.LABELS_SINGLES.length][trainingSet
				.numInstances()];
		// String labels[] = new String[trainingSet.numInstances()];
		int instIx[] = new int[Config.LABELS_SINGLES.length];
		Writer wr = Channels.newWriter(
				FileUtils.openOutputStream(
						FileUtils.getFile(outPath, "u"
								+ userId
								+ "_3d-data.csv")).getChannel(),
				Config.OUT_CHARSET);
		try {
			wr.append("ID\tAttr1\tAttr2\tAttr3\tLabel\tCluster\n");
			for (int i = 0; i < trainingSet.numInstances(); ++i) {
				Instance trainingInst = trainingSet.instance(i);
				// Instance noClassInst = noClassSet.instance(i);
				// if (trainingInst.value(0) != noClassInst.value(0)) {
				// throw new AssertionError("IDs don't match");
				// }
				int label = (int) Math.round(trainingInst.classValue());
				// long cluster = clusterer.clusterInstance(noClassInst);
				wr.append(Double.toString(instIx[label])).append('\t')
						.append(Double.toString(trainingInst.value(1)))
						.append('\t')
						.append(Double.toString(trainingInst.value(2)))
						.append('\t')
						.append(Double.toString(trainingInst.value(3)))
						.append('\t').append(Long.toString(label))
						// .append('\t').append(Long.toString(cluster))
						.append('\n');
				attr1[label][instIx[label]] = trainingInst.value(1);
				attr2[label][instIx[label]] = trainingInst.value(2);
				attr3[label][instIx[label]] = trainingInst.value(3);

				++instIx[label];
			}
		} finally {
			wr.flush();
			wr.close();
		}

		final ClusterIn3DSpace app = new ClusterIn3DSpace(userId);
		Plot3DPanel plot = new Plot3DPanel();
		app.add(plot);

		Color[] legend = new Color[] { Color.YELLOW, Color.BLUE, Color.CYAN,
				Color.RED, Color.GREEN, Color.MAGENTA, Color.DARK_GRAY,
				Color.GRAY, Color.BLACK, Color.ORANGE, Color.PINK };
		plot.addLegend(Plot3DPanel.EAST);

		String[] labelDescriptions = new String[] {
				"Insignificant", // 0
				"Home",
				"Home of a friend, relative or colleague",
				"My workplace/school",
				"Location related to transportation (bus stop,  metro stop,  train station,  parking lot,  airport)",
				"The workplace/school of a friend, relative or colleague",
				"Place for outdoor sports (e.g. walking,  hiking,  skiing)",
				"Place for indoor sports (e.g. gym)", "Restaurant or bar",
				"Shop or shopping center", "Holiday resort or vacation spot" };

		for (int l = 0; l < Config.LABELS_SINGLES.length; ++l) {
			if (instIx[l] == 0) {
				continue; // label not part of data
			}
			plot.addScatterPlot(labelDescriptions[l], legend[l],
					Arrays.copyOf(attr1[l], instIx[l]),
					Arrays.copyOf(attr2[l], instIx[l]),
					Arrays.copyOf(attr3[l], instIx[l]));
			// ((ScatterPlot) plot.getPlot(0)).setTags(labels);
			// plot.plotCanvas.allowNote = true;
			// plot.plotCanvas.allowNoteCoord = true;
		}

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				app.setVisible(true);
			}
		});

		// }
	}

}
