package uwaterloo.mdc.etl.util;

public class StringUtils {
	private StringUtils(){
		//no init
	}
	
	public static String removeLastNChars(String str, int n){
		return str.substring(0, str.length()-n);
	}
	
	public static String quote(String orig) {
		return "\"" + orig + "\"";
	}

	public static String numberToId(int number) {
		String userId;
		if (number < 10) {
			userId = "00" + number;
		} else if (number < 100) {
			userId = "0" + number;
		} else {
			userId = "" + number;
		}
		return userId;
	}
}
