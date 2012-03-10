package uwaterloo.mdc.etl.util;

import org.junit.Test;
import static org.junit.Assert.*;
public class MathUtilTest {

	@Test
	public void testPows2(){
		for(int i=0; i<MathUtil.POWS_OF_2.length; ++i){
			int actual = MathUtil.getPow2(Math.round(Math.pow(2,i)));
			assertEquals(i,actual);
		}
	}
}
