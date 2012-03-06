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

	private static final Map<Map<String, Writer>, Map<String, Thread>> writerToLocksMap = Collections
			.synchronizedMap(new HashMap<Map<String, Writer>, Map<String, Thread>>());

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
	public static final Writer acquireWriter(String filePath,
			Map<String, Writer> writersMap, String header) throws Exception {

		long delta = System.currentTimeMillis();

		Map<String, Thread> locksMap = getLocksMap(writersMap);

//		File file = FileUtils.getFile(outPath, fileName);
//		String filePath = file.getAbsolutePath();
		Writer writer = null;
		while (true) {
			// Loop to acquire the writer
			synchronized (writersMap) {
				if (!writersMap.containsKey(filePath)) {
					writer = Channels.newWriter(FileUtils
							.openOutputStream(FileUtils.getFile(filePath)).getChannel(),
							Config.OUT_CHARSET);
					writersMap.put(filePath, writer);

					writer.write(header);

					synchronized (locksMap) {
						locksMap.put(filePath, null);
						// Boolean.FALSE);
					}
				}

				Thread writerUser = null;
				synchronized (locksMap) {
					writerUser = locksMap.get(filePath);
					if (writerUser == null) {
						locksMap.put(filePath, Thread.currentThread());
					}
				}
				// Boolean isWriterInUse = Boolean.TRUE;
				// synchronized (locksMap) {
				// isWriterInUse = locksMap.get(filePath);
				// if (isWriterInUse == null || !isWriterInUse) {
				// isWriterInUse = Boolean.FALSE; // In case of null
				// locksMap.put(filePath,Boolean.TRUE); // Lock it!
				// }
				// }
				// if (!isWriterInUse) {
				if (writerUser == null) {
					writer = writersMap.get(filePath);
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
	 * @param filePath
	 * @param writersMap
	 * @param locksMap
	 * @throws IOException
	 */
	public static final void releaseWriter(Writer writer, String filePath,
			Map<String, Writer> writersMap, boolean close) throws IOException {
		long delta = System.currentTimeMillis();

		Map<String, Thread> locksMap = getLocksMap(writersMap);

		if (close) {
			synchronized (writersMap) {
				writersMap.remove(filePath);
				synchronized (locksMap) {
					locksMap.remove(filePath);
				}
				// We also flush and close the writer.. if we retain it, we
				// shouldn't
				writer.flush();
				writer.close();
			}
		} else {
			// In case of per feature counting, we need to retain the writers
			synchronized (locksMap) {
				locksMap.put(filePath, null);
			}
		}

		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
	}

	private static Map<String, Thread> getLocksMap(
			Map<String, Writer> writersMap) {
		Map<String, Thread> locksMap;
		synchronized (writerToLocksMap) {
			locksMap = writerToLocksMap.get(writersMap);
			if (locksMap == null) {
				locksMap = new HashMap<String, Thread>();
				writerToLocksMap.put(writersMap, locksMap);
			}
		}
		return locksMap;
	}
}
