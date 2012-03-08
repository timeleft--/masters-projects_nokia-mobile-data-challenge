package uwaterloo.mdc.etl.mallet;

import static org.junit.Assert.*;

import org.apache.commons.math.stat.Frequency;
import org.junit.Before;
import org.junit.Test;

import uwaterloo.mdc.etl.util.MathUtil;

public class RepeatedValTest {

	private Frequency appUsageFreq;
	@Before
	public void setUp(){
		appUsageFreq = new Frequency();
	}
	@Test
	public void testRepeatedVal(){
		String currValue = "BLAH";
//		appUsageFreq.addValue(currValue);
		for(int i=1;i<100;++i){
		appUsageFreq.addValue(currValue);
		long encounters = appUsageFreq.getCount(currValue);
		
		if(MathUtil.getPow2(encounters) <0){
			// In case of num > 1024, that's a stop word!
//			return null;
			continue;
		}
		assertTrue(
				(Math.log(i) / Math.log(2)) == MathUtil.getPow2(i));
//		long lgEnc = MathUtil.lgSmoothing(encounters);
		}
	}
}
