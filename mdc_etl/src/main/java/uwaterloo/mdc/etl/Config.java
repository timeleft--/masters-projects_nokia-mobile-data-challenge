package uwaterloo.mdc.etl;

public class Config {
	private Config(){
		//	Avoid creation
	}
	public static final String IN_CHARSET = "US-ASCII";
	public static final String OUT_CHARSET = "US-ASCII";
	
	public static final String USERID_COLNAME = "userid";
	public static final int NUM_THREADS = 1;
}
