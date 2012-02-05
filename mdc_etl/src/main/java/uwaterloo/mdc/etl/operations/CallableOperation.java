package uwaterloo.mdc.etl.operations;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.stats.CalcPerUserStats;

@SuppressWarnings("unused")
public abstract class CallableOperation<V> implements
		Callable<HashMap<String, V>> {

	// Sadly, this didn't work because the writer didn't translate the character
	// encoding as it should, and I couldn't use ASCII because java get confused
	// when there is one character byte at the end of the file (is it EOF??)

	// protected class InputWorker implements Callable<Void> {
	//
	// @Override
	// public Void call() throws Exception {
	// // CharBuffer buffer = CharBuffer.allocate(bufferSize);
	// //For debugging only
	// char[] buffArr = new char[bufferSize];
	// CharBuffer buffer = CharBuffer.wrap(buffArr);
	// long len = 0;
	// while ((len = inReader.read(buffer)) != -1) {
	// buffer.flip();
	// sink.append(buffer);
	// buffer.clear();
	// }
	// return null;
	// }
	//
	// }

	public static final char DEFAULT_DELIMITER = '\t';
	public static final int DEFAULT_BUFF_SIZE = 4 * 1024;
	public static final String DEFAULT_EOL = "\n";// System.getProperty("line.separator");

	private final char delimiter;
	private final String eol;
	private final char eol0; // For performance boost when eol is \n
	private final int bufferSize;
	// // private final Reader source;
	// private final ReadableByteChannel source;
	// private final Writer sink;
	// private final InputWorker inputWorker;
	private final Reader inReader;
	private FileInputStream inStream;

	protected/* final */HashMap<String, V> result;
	protected final LinkedList<String> keyList;
	protected Iterator<String> keyIterator;

	private final StringBuilder currValueBuilder;
	protected String currValue;
	protected String currKey;
	protected ArrayList<String> tuple;

	protected final CalcPerUserStats master;
	protected final String outPath;
	protected final File dataFile;
	protected CharSequence userid;

	@Deprecated
	public CallableOperation(CalcPerUserStats master, char delimiter,
			String eol, int bufferSize, File dataFile, String outPath)
			throws IOException {
		try {
			this.master = master;
			this.outPath = outPath;
			this.dataFile = dataFile;

			this.delimiter = delimiter;
			this.eol = eol;
			this.eol0 = eol.charAt(0);
			this.bufferSize = bufferSize;

			inStream = FileUtils.openInputStream(dataFile);
			inReader = Channels.newReader(inStream.getChannel(),
					Config.IN_CHARSET);
			// this.inputWorker = new InputWorker();
			// Pipe p = Pipe.open();
			// this.sink = Channels.newWriter(p.sink(), Config.OUT_CHARSET);
			// // this.source = Channels.newReader(p.source(),
			// Config.OUT_CHARSET);
			// this.source = p.source();

			currValueBuilder = new StringBuilder();
			keyList = new LinkedList<String>();

			result = new HashMap<String, V>();

			char[] buff = new char[1];
			long len = 0;
			char eolchi = eol0;
			int eoli = 1;

			while ((len = inReader.read(buff)) != -1) {
				if (buff[0] == eolchi) {
					if (eol.length() > eoli) {
						eolchi = eol.charAt(eoli++);
					} else {
						eoli = 1;
						eolchi = eol0;
						// EOL is also a delimiter:
						headerDelimiterProcedureInternal();
						headerEolProcedure();
						break;
					}
				} else if (buff[0] == delimiter) {
					headerDelimiterProcedureInternal();
				} else {
					currValueBuilder.append(buff[0]);
				}

			}
		} catch (Exception ex) {
			if (inStream != null) {
				inStream.close();
			}
			throw ex;
		}
	}

	@Override
	public HashMap<String, V> call() throws Exception {
		try {
			// ExecutorService inputExec;
			// Future<Void> inputFuture;
			// inputExec = Executors.newSingleThreadExecutor();
			// inputFuture = inputExec.submit(inputWorker);

			char[] buff = new char[bufferSize];
			CharBuffer buffer = CharBuffer.wrap(buff);

			long len = 0;
			char eolchi = eol0;
			int eoli = 1;

			while (true) {
				long delta = System.currentTimeMillis();
				len = inReader.read(buffer);
				delta = System.currentTimeMillis() - delta;
				PerfMon.increment(TimeMetrics.IO_READ, delta);
				if (len == -1) {
					break;
				}

				buffer.flip();
				for (int i = 0; i < len; ++i) {
					char ch = buff[i];
					if (ch == eolchi) {
						if (eol.length() > eoli) {
							eolchi = eol.charAt(eoli++);
						} else {
							eoli = 1;
							eolchi = eol0;
							// EOL is also a delimiter:
							delimiterProcedureInternal();
							eolProcedure();
						}
					} else if (ch == delimiter) {
						delimiterProcedureInternal();
					} else {
						currValueBuilder.append(ch);
					}
				}
				buffer.clear();
			}

			writeResults();

			PerfMon.increment(TimeMetrics.FILES_PROCESSED, 1);
			return result;
		} finally {
			if (inStream != null) {
				inStream.close();
			}
		}
	}

	private void delimiterProcedureInternal() {
		currValue = currValueBuilder.toString().trim();
		currValueBuilder.setLength(0);
		tuple.add(currValue);
		assert keyIterator.hasNext() : "The columns seem to be less than the values in this row!!";
		currKey = keyIterator.next();
		if (Config.USERID_COLNAME.equals(currKey)) {
			if (userid == null) {
				userid = currValue;
			}
			return; // So that we don't count userid
		}
		delimiterProcedure();
	}

	private void headerDelimiterProcedureInternal() {
		currValue = currValueBuilder.toString().trim();
		currValueBuilder.setLength(0);
		keyList.add(currValue);
		if (Config.USERID_COLNAME.equals(currValue)) {
			return; // So that we don't have one extra frequency
		}
		headerDelimiterProcedure();
	}

	/**
	 * Subclasses can override this to do something with the keyList
	 */
	protected void headerEolProcedure() {
		eolProcedure();
	}

	/**
	 * Subclasses can override this to do something with the whole tuple
	 */
	protected void eolProcedure() {
		keyIterator = keyList.iterator();
		tuple = new ArrayList<String>(keyList.size());
	}

	/**
	 * Subclasses can override this to fill the map with column objects
	 */
	protected abstract void headerDelimiterProcedure();

	/**
	 * Subclasses can override this to act on values
	 */
	protected abstract void delimiterProcedure();

	/**
	 * Subclasses must override this to write their results out
	 * 
	 * @throws IOException
	 */
	protected abstract void writeResults() throws IOException;

	/**
	 * Subclasses must override this to create the header of output file
	 * 
	 * @return header of output file
	 */
	protected abstract String getHeaderLine();

	protected final Writer acquireWriter(String freqFileName,
			Map<String, Writer> writersMap, Map<String, Boolean> locksMap)
			throws IOException {
		long delta = System.currentTimeMillis();
		Writer writer = null;
		while (true) {
			// Loop to acquire the writer
			synchronized (writersMap) {
				if (!writersMap.containsKey(freqFileName)) {
					writer = Channels.newWriter(
							FileUtils.openOutputStream(
									FileUtils.getFile(outPath, freqFileName))
									.getChannel(), Config.OUT_CHARSET);
					writersMap.put(freqFileName, writer);

					String header = getHeaderLine();
					writer.write(header);

					synchronized (locksMap) {
						locksMap.put(freqFileName, Boolean.FALSE);
					}
				}

				Boolean isWriterInUse = Boolean.TRUE;
				synchronized (locksMap) {
					isWriterInUse = locksMap.get(freqFileName);
					if (!isWriterInUse) {
						locksMap.put(freqFileName, Boolean.TRUE);
					}
				}
				if (!isWriterInUse) {
					writer = writersMap.get(freqFileName);
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

	protected final void releaseWriter(Writer writer, String freqFileName,
			Map<String, Writer> writersMap, Map<String, Boolean> locksMap)
			throws IOException {
		long delta = System.currentTimeMillis();

		// In case of per feature counting, we need to retain the writers
		// synchronized (locksMap) {
		// locksMap.put(freqFileName, Boolean.FALSE);
		// }

		synchronized (writersMap) {
			writersMap.remove(freqFileName);
			synchronized (locksMap) {
				locksMap.remove(freqFileName);
			}
		}
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.WAITING_LOCK, delta);
		
		// We also flush and close the writer.. if we retain it, we shouldn't
		writer.flush();
		writer.close();
	}

	public char getDelimiter() {
		return delimiter;
	}

	public String getEol() {
		return eol;
	}

	public char getEol0() {
		return eol0;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, V> getResult() {
		return (HashMap<String, V>) result.clone();
	}

	@SuppressWarnings("unchecked")
	public LinkedList<String> getKeyList() {
		return (LinkedList<String>) keyList.clone();
	}

	public String getCurrValue() {
		return currValue;
	}

	public String getCurrKey() {
		return currKey;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<String> getTuple() {
		return (ArrayList<String>) tuple.clone();
	}

}
