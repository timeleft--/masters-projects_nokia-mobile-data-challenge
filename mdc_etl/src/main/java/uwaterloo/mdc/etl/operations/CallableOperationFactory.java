package uwaterloo.mdc.etl.operations;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;

import uwaterloo.mdc.etl.PerfMon;
import uwaterloo.mdc.etl.PerfMon.TimeMetrics;

/**
 * This factory allows for different handling for different files within a user's file set.
 * It looks for a class having the same name of the operations class prototype, but with the 
 * data file name postfixed to it. It must be in the same package as the prototype class. 
 * If such class exists it instantiates it, otherwise it instantiates the prototype class.  
 * 
 * @author yaboulna
 *
 * @param <R>
 * @param <V>
 */
public class CallableOperationFactory<R, V> {
	
	private HashMap<String, Class<? extends CallableOperation<R,V>>> resultClazzes = new HashMap<String, Class<? extends CallableOperation<R,V>>>();
	
	public CallableOperation<R,V> createOperation(Class<? extends CallableOperation<R,V>> prototype, Object master,
			File dataFile, String outPath) throws Exception {
		return createOperation(prototype, master, CallableOperation.DEFAULT_DELIMITER,
				CallableOperation.DEFAULT_EOL,
				CallableOperation.DEFAULT_BUFF_SIZE, dataFile, outPath);
	}

	@SuppressWarnings("unchecked")
	public CallableOperation<R,V> createOperation(Class<? extends CallableOperation<R,V>> prototype, Object master,
			char delimiter, String eol, int bufferSize, File dataFile,
			String outPath) throws Exception {
		long delta = System.currentTimeMillis();
		Class<? extends CallableOperation<R,V>> resultClazz = loadClass(prototype, dataFile.getName());

		
		Constructor<? extends CallableOperation<R,V>> constructor = (Constructor<? extends CallableOperation<R,V>>) resultClazz.getConstructors()[0];
		CallableOperation<R,V> result = constructor.newInstance(master,
			delimiter, eol, bufferSize, dataFile,
			outPath);
		
		result.initHeader();
		
		delta = System.currentTimeMillis() - delta;
		PerfMon.increment(TimeMetrics.REFLECTION, delta);
		
		return result;
	}
	
	public Class<? extends CallableOperation<R, V>> loadClass(Class<? extends CallableOperation<R,V>> prototype, String dataFileName){
		String resultClassName = prototype.getCanonicalName() + "_" + FilenameUtils.removeExtension(dataFileName);
		Class<? extends CallableOperation<R,V>> resultClazz = resultClazzes.get(resultClassName);
		if(resultClazz == null){
			try{
			resultClazz = (Class<? extends CallableOperation<R,V>>) Class.forName(resultClassName);
			resultClazzes.put(resultClassName, resultClazz);
			}catch(ClassNotFoundException e){
				resultClazz = prototype;
			}
		}
		return resultClazz;
	}
}
