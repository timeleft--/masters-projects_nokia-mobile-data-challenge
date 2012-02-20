package uwaterloo.mdc.etl.util;

import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;

public class ConcurrUtil {

	private static final Map<Map<String, Writer>, Map<String, Boolean>> writerToLocksMap = Collections
			.synchronizedMap(new HashMap<Map<String, Writer>, Map<String, Boolean>>());

	private ConcurrUtil() {
		// no init
	}

	/**
	 * To standardize the way different threads acquire writers, they must go
	 * through this method. No guards are taken to enforce this!
	 * 
	 * @param fileName
	 * @param writersMap
	 * @param locksMap
	 * @return
	 * @throws IOException
	 */
	public static final Writer acquireWriter(String outPath, String fileName,
			Map<String, Writer> writersMap, String header)
			throws Exception {
		long delta = System.currentTimeMillis();

		Map<String, Boolean> locksMap = getLocksMap(writersMap);

		Writer writer = null;
		while (true) {
			// Loop to acquire the writer
			synchronized (writersMap) {
				if (!writersMap.containsKey(fileName)) {
					writer = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, fileName))
									.getChannel(), Config.OUT_CHARSET);
					writersMap.put(fileName, writer);

					writer.write(header);

					synchronized (locksMap) {
						locksMap.put(fileName, Boolean.FALSE);
					}
				}

				Boolean isWriterInUse = Boolean.TRUE;
				synchronized (locksMap) {
					isWriterInUse = locksMap.get(fileName);
					if (isWriterInUse == null || !isWriterInUse) {
						isWriterInUse =  Boolean.TRUE;
						locksMap.put(fileName,isWriterInUse);
					}
				}
				if (!isWriterInUse) {
					writer = writersMap.get(fileName);
				}
			}
			if (writer == null) {
				try {
					Thread.sleep(100);
					delta += 100; // We will add them immediately to be visible!
					PerfMon.increment(TimeMetrics.WAITING_LOCK, 100);
				} catch (InterruptedException e) {
					// Probably the program is shutting down
					break;
				}
			} else {
				break;
			}
		}
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
		return writer;
	}

	/**
	 * To standardize the way different threads release writers, they must go
	 * through this method. No guards are taken to enforce this!
	 * 
	 * @param writer
	 * @param fileName
	 * @param writersMap
	 * @param locksMap
	 * @throws IOException
	 */
	public static final void releaseWriter(Writer writer, String fileName,
			Map<String, Writer> writersMap, boolean close) throws IOException {
		long delta = System.currentTimeMillis();

		Map<String, Boolean> locksMap = getLocksMap(writersMap);

		if (close) {
			synchronized (writersMap) {
				writersMap.remove(fileName);
				synchronized (locksMap) {
					locksMap.remove(fileName);
				}
				// We also flush and close the writer.. if we retain it, we
				// shouldn't
				writer.flush();
				writer.close();
			}
		} else {
			// In case of per feature counting, we need to retain the writers
			synchronized (locksMap) {
				locksMap.put(fileName, Boolean.FALSE);
			}
		}

		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
	}

	private static Map<String, Boolean> getLocksMap(
			Map<String, Writer> writersMap) {
		Map<String, Boolean> locksMap;
		synchronized (writerToLocksMap) {
			locksMap = writerToLocksMap.get(writersMap);
			if (locksMap == null) {
				locksMap = new HashMap<String, Boolean>();
				writerToLocksMap.put(writersMap, locksMap);
			}
		}
		return locksMap;
	}
}
