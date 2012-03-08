package uwaterloo.mdc.etl.util;

import java.util.Arrays;

public class MathUtil {
	public static final double LG2 = Math.log(2);
	public static final long[] pows2 = new long[10];
	static {
		for(int i = 0; i<pows2.length; ++i){
			pows2[i] = Math.round(Math.pow(2, i));
		}
	}
	private MathUtil() {

	}

	public static long lgSmoothing(long orig) {
		return 1 + Math.round(Math.floor((Math.log(orig) / LG2)));
	}

	public static int getPow2(long num) {
		return Arrays.binarySearch(pows2, num);
	}
}
