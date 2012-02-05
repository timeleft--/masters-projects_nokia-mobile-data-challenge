package uwaterloo.mdc.etl;

public class PerfMon {
	private PerfMon(){
		//Avoid initializing
	}
	public enum TimeMetrics {WAITING_LOCK, IO_READ,IO_WRITE, FILES_PROCESSED, REFLECTION};
	private static final long[] timeMeasures = new long[TimeMetrics.values().length];
	
	public static final long startTime = System.currentTimeMillis();
	
	public static final synchronized void increment(TimeMetrics metric, long delta){
		assert delta >= 0: "Gotcha!";
		timeMeasures[metric.ordinal()]+=delta;
	}

	public static String asString() {
		StringBuilder resBuilder = new StringBuilder();
		resBuilder.append("Millis since start: ").append(System.currentTimeMillis() - startTime);
		resBuilder.append(" - Num Threads: ").append(Config.NUM_THREADS);
		for(TimeMetrics metric: TimeMetrics.values()){
			resBuilder.append(" - ").append(metric.toString()).append(": ").append(timeMeasures[metric.ordinal()]);
		}
		return resBuilder.toString();
	}
}
