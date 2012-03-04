package uwaterloo.mdc.etl.mallet;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uwaterloo.mdc.etl.Config;
import cc.mallet.types.Instance;

public class VisitIteratorTest {

	private File userDir;
	private VisitsIterator target;

	@BeforeClass
	public static void setUpClass() throws Exception {
		Config.placeLabels = new Properties();
		Config.placeLabels.load(FileUtils.openInputStream(FileUtils
				.getFile(Config.PATH_PLACE_LABELS_PROPERTIES_FILE)));
	}
	
	@Before
	public void setUp() throws Exception {
		URL userDirURI = this.getClass().getResource("/user-visits/001");
		String userDirPath = userDirURI.getPath();
		userDir = FileUtils.getFile(userDirPath);
		
		target = new VisitsIterator(userDir);
	}

	@Test
	public void testNext() {
		while(target.hasNext()){
			Instance inst = target.next();
			System.out.println(inst.getName().toString() + ": " + inst.getTarget().toString());
		}
	}

	
}
