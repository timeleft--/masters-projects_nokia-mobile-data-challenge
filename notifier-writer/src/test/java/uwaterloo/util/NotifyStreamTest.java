package uwaterloo.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

public class NotifyStreamTest {

	@Test
	public void test() throws InterruptedException, IOException {
		PrintStream err = System.err;
		NotifyStream notifyStream = new NotifyStream(err, "Test");
		try{
		System.setErr(new PrintStream(notifyStream));
		System.err.println("Go find this at plg.uwaterloo.ca");
//		while(true) //debug
//		Thread.sleep(10000);
		}finally{
			notifyStream.close();
		}
	}

}
