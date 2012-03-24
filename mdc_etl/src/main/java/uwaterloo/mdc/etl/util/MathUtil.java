package uwaterloo.mdc.etl.util;

import java.util.Arrays;

public class MathUtil {
	public static final double LOG_OF_2 = Math.log(2);
	public static final long[] POWS_OF_2 = new long[8]; //0 -> 128
	static {
		for(int i = 0; i<POWS_OF_2.length; ++i){
			POWS_OF_2[i] = Math.round(Math.pow(2, i));
		}
	}
	private MathUtil() {

	}

	public static long tf(double orig) {
		if(orig == 0){
			return 0;
		}
		return 1 + Math.round(lg2(orig)); //Math.floor(
	}

	public static int getPow2(long num) {
		return Arrays.binarySearch(POWS_OF_2, num);
	}

	public static long tfIdf(int inDocFreq, int numDocsAppearing, int totalNumDocs) {
		assert totalNumDocs != 0;
		if(numDocsAppearing == 0 || inDocFreq == 0){
			return 0;
		}
		return Math.round((1 + lg2(inDocFreq)) * lg2(totalNumDocs / numDocsAppearing));
	}
	
	public static double lg2(double orig){
		return (Math.log10(orig) / LOG_OF_2);
	}
}
