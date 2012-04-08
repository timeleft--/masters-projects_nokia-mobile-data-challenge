package uwaterloo.mdc.weka;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Random;

import weka.classifiers.lazy.kstar.KStarCache;
import weka.classifiers.lazy.kstar.KStarNominalAttribute;
import weka.classifiers.lazy.kstar.KStarNumericAttribute;
import weka.core.Attribute;
import weka.core.DistanceFunction;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.PerformanceStats;

public class EntropyDistanceMetric implements DistanceFunction {

	int NUM_RAND_COLS = 5;

	Instances m_Train;
	int m_NumAttributes;
	int m_NumInstances;
	/** Table of random class value colomns */
	protected int[][] m_RandClassCols;
	/**
	 * A custom data structure for caching distinct attribute values and their
	 * scale factor or stop parameter.
	 */
	protected KStarCache[] m_Cache;

	public Enumeration listOptions() {
		return Collections.enumeration(Collections.EMPTY_LIST);
	}

	public void setOptions(String[] options) throws Exception {

	}

	public String[] getOptions() {
		return new String[] {};
	}

	public void setInstances(Instances insts) {

	}

	public EntropyDistanceMetric(Instances insts) {
		m_Train = insts;
		m_NumAttributes = insts.numAttributes();
		m_NumInstances = insts.numInstances();
		m_Cache = new KStarCache[m_NumAttributes];
		for (int i = 0; i < m_NumAttributes; i++) {
			m_Cache[i] = new KStarCache();
		}
	}

	public Instances getInstances() {
		throw new UnsupportedOperationException();
	}

	public void setAttributeIndices(String value) {
		throw new UnsupportedOperationException();

	}

	public String getAttributeIndices() {
		throw new UnsupportedOperationException();
	}

	public void setInvertSelection(boolean value) {
		throw new UnsupportedOperationException();
	}

	public boolean getInvertSelection() {
		throw new UnsupportedOperationException();
	}

	public double distance(Instance first, Instance second) {
		return instanceTransformationProbability(first, second);
	}

	/**
	 * Calculate the probability of the first instance transforming into the
	 * second instance: the probability is the product of the transformation
	 * probabilities of the attributes normilized over the number of instances
	 * used.
	 * 
	 * @param first
	 *            the test instance
	 * @param second
	 *            the train instance
	 * @return transformation probability value
	 */
	private double instanceTransformationProbability(Instance first,
			Instance second) {
		String debug = "(KStar.instanceTransformationProbability) ";
		double transProb = 1.0;
		int numMissAttr = 0;
		for (int i = 0; i < m_NumAttributes; i++) {
			if (i == m_Train.classIndex()) {
				continue; // ignore class attribute
			}
			if (first.isMissing(i)) { // test instance attribute value is
										// missing
				numMissAttr++;
				continue;
			}
			transProb *= attrTransProb(first, second, i);
			// normilize for missing values
			if (numMissAttr != m_NumAttributes) {
				transProb = Math.pow(transProb, (double) m_NumAttributes
						/ (m_NumAttributes - numMissAttr));
			} else { // weird case!
				transProb = 0.0;
			}
		}
		// normilize for the train dataset
		return transProb / m_NumInstances;
	}

	/**
	 * Calculates the transformation probability of the indexed test attribute
	 * to the indexed train attribute.
	 * 
	 * @param first
	 *            the test instance.
	 * @param second
	 *            the train instance.
	 * @param col
	 *            the index of the attribute in the instance.
	 * @return the value of the transformation probability.
	 */
	private double attrTransProb(Instance first, Instance second, int col) {
		String debug = "(KStar.attrTransProb)";
		double transProb = 0.0;
		KStarNominalAttribute ksNominalAttr;
		KStarNumericAttribute ksNumericAttr;
		switch (m_Train.attribute(col).type()) {
		case Attribute.NOMINAL:
			ksNominalAttr = new KStarNominalAttribute(first, second, col,
					m_Train, m_RandClassCols, m_Cache[col]);
			// Use defaults for now ksNominalAttr.setOptions(m_MissingMode,
			// m_BlendMethod, m_GlobalBlend);
			transProb = ksNominalAttr.transProb();
			ksNominalAttr = null;
			break;

		case Attribute.NUMERIC:
			ksNumericAttr = new KStarNumericAttribute(first, second, col,
					m_Train, m_RandClassCols, m_Cache[col]);
			// defaults ksNumericAttr.setOptions(m_MissingMode, m_BlendMethod,
			// m_GlobalBlend);
			transProb = ksNumericAttr.transProb();
			ksNumericAttr = null;
			break;
		}
		return transProb;
	}

	/**
	 * Note: for Nominal Class Only! Generates a set of random versions of the
	 * class colomn.
	 */
	private void generateRandomClassColomns() {
		String debug = "(KStar.generateRandomClassColomns)";
		Random generator = new Random(42);
		// Random generator = new Random();
		m_RandClassCols = new int[NUM_RAND_COLS + 1][];
		int[] classvals = classValues();
		for (int i = 0; i < NUM_RAND_COLS; i++) {
			// generate a randomized version of the class colomn
			m_RandClassCols[i] = randomize(classvals, generator);
		}
		// original colomn is preserved in colomn NUM_RAND_COLS
		m_RandClassCols[NUM_RAND_COLS] = classvals;
	}

	/**
	 * Note: for Nominal Class Only! Returns an array of the class values
	 * 
	 * @return an array of class values
	 */
	private int[] classValues() {
		String debug = "(KStar.classValues)";
		int[] classval = new int[m_NumInstances];
		for (int i = 0; i < m_NumInstances; i++) {
			try {
				classval[i] = (int) m_Train.instance(i).classValue();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return classval;
	}

	/**
	 * Returns a copy of the array with its elements randomly redistributed.
	 * 
	 * @param array
	 *            the array to randomize.
	 * @param generator
	 *            the random number generator to use
	 * @return a copy of the array with its elements randomly redistributed.
	 */
	private int[] randomize(int[] array, Random generator) {
		String debug = "(KStar.randomize)";
		int index;
		int temp;
		int[] newArray = new int[array.length];
		System.arraycopy(array, 0, newArray, 0, array.length);
		for (int j = newArray.length - 1; j > 0; j--) {
			index = (int) (generator.nextDouble() * (double) j);
			temp = newArray[j];
			newArray[j] = newArray[index];
			newArray[index] = temp;
		}
		return newArray;
	}

	public double distance(Instance first, Instance second,
			PerformanceStats stats) throws Exception {
		throw new UnsupportedOperationException();
	}

	public double distance(Instance first, Instance second, double cutOffValue) {
		throw new UnsupportedOperationException();
	}

	public double distance(Instance first, Instance second, double cutOffValue,
			PerformanceStats stats) {
		throw new UnsupportedOperationException();
	}

	public void postProcessDistances(double[] distances) {
		throw new UnsupportedOperationException();
	}

	public void update(Instance ins) {
		throw new UnsupportedOperationException();
	}

}
