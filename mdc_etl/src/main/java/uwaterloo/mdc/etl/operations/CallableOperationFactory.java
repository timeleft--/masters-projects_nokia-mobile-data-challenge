package uwaterloo.mdc.etl.operations;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;

import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;
import uwaterloo.mdc.stats.CalcPerUserStats;

public class CallableOperationFactory<V> {
	
	private HashMap<String, Class<? extends CallableOperation<V>>> resultClazzes = new HashMap<String, Class<? extends CallableOperation<V>>>();
	
	public CallableOperation<V> createOperation(Class<? extends CallableOperation<V>> prototype, CalcPerUserStats master,
			File dataFile, String outPath) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return createOperation(prototype, master, CallableOperation.DEFAULT_DELIMITER,
				CallableOperation.DEFAULT_EOL,
				CallableOperation.DEFAULT_BUFF_SIZE, dataFile, outPath);
	}

	@SuppressWarnings("unchecked")
	public CallableOperation<V> createOperation(Class<? extends CallableOperation<V>> prototype, CalcPerUserStats master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		long delta = System.currentTimeMillis();
		String resultClassName = prototype.getCanonicalName() + "_" + FilenameUtils.removeExtension(dataFile.getName());
		Class<? extends CallableOperation<V>> resultClazz = resultClazzes.get(resultClassName);
		if(resultClazz == null){
			try{
			resultClazz = (Class<? extends CallableOperation<V>>) Class.forName(resultClassName);
			resultClazzes.put(resultClassName, resultClazz);
			}catch(ClassNotFoundException e){
				resultClazz = prototype;
			}
		}
		
		Constructor<? extends CallableOperation<V>> constructor = (Constructor<? extends CallableOperation<V>>) resultClazz.getConstructors()[0];
		CallableOperation<V> result = constructor.newInstance(master,
			delimiter, eol, bufferSize, dataFile,
			outPath);
		
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.REFLECTION, delta);
		
		return result;
	}
}
