package uwaterloo.mdc.etl.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class TimeSearchTest {

	private File userDir;
	UserVisitsDocsHierarchy<Object> target;

	@Before
	public void setUp() throws NoSuchMethodException, SecurityException, Exception {
		URL userDirURI = this.getClass().getResource("/user-visits/001");
		String userDirPath = userDirURI.getPath();
		userDir = FileUtils.getFile(userDirPath);
		target = new UserVisitsDocsHierarchy<Object>(userDir, Object.class.getConstructor());

	}

	@Test
	public void testExistingExact() {
		
		for (File visitDir : userDir.listFiles()) {
			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visitDir.getName(), 1));
			if (!visitDir.isDirectory()) {
				continue;
			}

			Visit<Object> visit = target
					.searchInVisit(visitStart, true);
			assertEquals("Wrong visit", visitStart, visit.getKey());

			for (File microLoc : visitDir.listFiles()) {
				Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
						microLoc.getName(), 5));

				KeyValuePair<Long, Object> doc = target
						.searchInMicroLocs(visit, docEndTime, true);

				assertNotNull(String.format(
						"Cannot find the existing visit %d or microloc %d",
						visitStart, docEndTime), doc);
				assertEquals("Wrong Microloc", docEndTime, doc.getKey());
			}
		}
	}

	@Test
	public void testExistingInexactVisit() {
		
		File[] visits = userDir.listFiles();
		for (int i = 0; i < visits.length; ++i) {
			if (!visits[i].isDirectory()) {
				continue;
			}

			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visits[i].getName(), 1));
			Long inexactStart = visitStart + 1;

			KeyValuePair<Long, ArrayList<KeyValuePair<Long, Object>>> actualVisit = target
					.searchInVisit(inexactStart, false);
			assertNotNull(String.format(
					"Cannot find the inexact visit (%d,%d)", visitStart,
					inexactStart), actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());
		}
	}

	@Test
	public void testExistingInexactMicroLoc() {
		
		for (File visit : userDir.listFiles()) {
			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visit.getName(), 1));
			if (!visit.isDirectory()) {
				continue;
			}

			Visit<Object> actualVisit = target
					.searchInVisit(visitStart, true);
			assertNotNull(String.format("Cannot find the exact visit %d",
					visitStart), actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());

			Long prevDocEndTime = visitStart;
			for (File microLoc : visit.listFiles()) {
				Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
						microLoc.getName(), 5));


				long inexactEndTime = prevDocEndTime + 1;

				KeyValuePair<Long, Object> actualDoc = target
						.searchInMicroLocs(actualVisit, inexactEndTime, false);

				assertNotNull(String.format(
						"Cannot find the inexact microloc (%d:%d,%d)",
						visitStart, docEndTime, inexactEndTime), actualDoc);

				assertEquals("Wrong Microloc", docEndTime, actualDoc.getKey());
				prevDocEndTime = docEndTime;
			}
		}
	}

	@Test
	public void testExistingApproxStart() {
		
		File[] visits = userDir.listFiles();
		int skipCount = 0;
		for (int i = 1; i < visits.length; ++i) {
			if (!visits[i].isDirectory()) {
				continue;
			}

			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visits[i].getName(), 1));
			Long inexactStart = visitStart
					- Discretize.getStartEndTimeError(StringUtils
							.charAtFromEnd(visits[i].getName(), 1)) + 1;
			File[] prevVisitFiles = visits[i-1].listFiles();
			File prevVisitEndDoc =prevVisitFiles[prevVisitFiles.length - 1];
			long prevVisitEndTime = Long.parseLong(StringUtils
					.removeLastNChars(prevVisitEndDoc.getName(), 5))
					+ Discretize.getStartEndTimeError(StringUtils
							.charAtFromEnd(prevVisitEndDoc.getName(), 5));
			if (prevVisitEndTime >= inexactStart) {
				++skipCount;
				continue; // another correct solution
			}

			Visit<Object> actualVisit = target
					.searchInVisit(inexactStart, false);
			assertNotNull(String.format("Cannot find the approx visit (%d,%d)",
					visitStart, inexactStart), actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());
		}
		assertTrue("Skipped all!", skipCount < visits.length);
	}

	@Test
	public void testExistingApproxEnd() {
		
		File[] visits = userDir.listFiles();
		int skipCount = 0;
		for (int i = 0; i < visits.length; ++i) {
			if (!visits[i].isDirectory()) {
				continue;
			}

			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visits[i].getName(), 1));

			Visit<Object> actualVisit = target
					.searchInVisit(visitStart, true);
			assertNotNull(String.format("Cannot find the exact visit %d",
					visitStart), actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());

			File[] microLocs = visits[i].listFiles();
			int j = microLocs.length - 1;
			Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
					microLocs[j].getName(), 5));

			long inexactEndTime = docEndTime
					+ Discretize.getStartEndTimeError(StringUtils
							.charAtFromEnd(microLocs[j].getName(), 5)) - 1;

			if (i < visits.length - 1) {
				long nextVisitStratTime = Long.parseLong(StringUtils
						.removeLastNChars(visits[i + 1].getName(), 1))
						- Discretize.getStartEndTimeError(StringUtils
								.charAtFromEnd(visits[i + 1].getName(), 1));
				if (nextVisitStratTime <= inexactEndTime) {
					++skipCount;
					continue; // another correct solution
				}
			}

			KeyValuePair<Long, Object> actualDoc = target
					.searchInMicroLocs(actualVisit, inexactEndTime, false);

			assertNotNull(String.format(
					"Cannot find the approx microloc (%d:%d,%d)", visitStart,
					docEndTime, inexactEndTime), actualDoc);

			assertEquals("Wrong Microloc", docEndTime, actualDoc.getKey());

		}
		assertTrue("Skipped all!", skipCount < visits.length);
	}

	@Test
	public void testNonexistingApproxStart() {
		
		File[] visits = userDir.listFiles();
		int i = 0;
		while (!visits[i].isDirectory()) {
			++i;
		}

		Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
				visits[i].getName(), 1));
		Long inexactStart = visitStart
				- Discretize.getStartEndTimeError(StringUtils.charAtFromEnd(
						visits[i].getName(), 1)) - 1;

		Visit<Object> actualVisit = target
				.searchInVisit(inexactStart, false);
		if (actualVisit != null) {
			fail(String.format("Found a visit %d for search time %d",
					actualVisit.getKey(), inexactStart));
		}

	}

	@Test
	public void testNonexistingApproxEnd() {
		
		File[] visits = userDir.listFiles();
		int skipCount = 0;
		for (int i = 0; i < visits.length; ++i) {
			if (!visits[i].isDirectory()) {
				continue;
			}

			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visits[i].getName(), 1));

			Visit<Object> actualVisit = target
					.searchInVisit(visitStart, false);
			assertNotNull(String.format("Cannot find the inexact visit %d",
					visitStart), actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());

			File[] microLocs = visits[i].listFiles();
			int j = microLocs.length - 1;
			Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
					microLocs[j].getName(), 5));

			long inexactEndTime = docEndTime
					+ Discretize.getStartEndTimeError(StringUtils
							.charAtFromEnd(microLocs[j].getName(), 5)) + 1;

			if (i < visits.length - 1) {
				long nextVisitStratTime = Long.parseLong(StringUtils
						.removeLastNChars(visits[i + 1].getName(), 1))
						- Discretize.getStartEndTimeError(StringUtils
								.charAtFromEnd(visits[i + 1].getName(), 1));
				if (nextVisitStratTime <= inexactEndTime) {
					++skipCount;
					continue; // another correct solution

				}
			}

			KeyValuePair<Long, Object> actualDoc = target
					.searchInMicroLocs(actualVisit, inexactEndTime, false);

			if (actualDoc != null) {
				fail(String.format("Found a microloc %d:%d for search time %d",
						visitStart, actualDoc.getKey(), inexactEndTime));
			}

		}
		assertTrue("Skipped all!", skipCount < visits.length);
	}

	@Test
	public void testNotExistingDoc() {
		
		File[] visits = userDir.listFiles();
		int skipCount = 0;
		for (int i = 0; i < visits.length; ++i) {
			if (!visits[i].isDirectory()) {
				continue;
			}

			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visits[i].getName(), 1));
			File[] microLocs = visits[i].listFiles();
			int j = microLocs.length - 1;
			Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
					microLocs[j].getName(), 5));

			long error = Discretize.getStartEndTimeError(StringUtils
					.charAtFromEnd(microLocs[j].getName(), 5));
			long inexactEndTime = docEndTime + error + 1;

			if (i < visits.length - 1) {
				long nextVisitStratTime = Long.parseLong(StringUtils
						.removeLastNChars(visits[i + 1].getName(), 1))
						- Discretize.getStartEndTimeError(StringUtils
								.charAtFromEnd(visits[i + 1].getName(), 1));
				if (nextVisitStratTime <= inexactEndTime) {
					++skipCount;
					continue; // there will be a correct document
				}
			}

			Object doc = target.getDocForEndTime(inexactEndTime);

			assertNull(String.format(
					"Found the exact visit (%s,%d) or approx microloc (%s,%d)",
					visits[i], visitStart, microLocs[j], docEndTime), doc);

		}
		assertTrue("Skipped all!", skipCount < visits.length);
	}

	@Test
	public void testExistingDoc() {
		
		Random rand = new Random();
		for (File visit : userDir.listFiles()) {
			Long visitStart = Long.parseLong(StringUtils.removeLastNChars(
					visit.getName(), 1));
			if (!visit.isDirectory()) {
				continue;
			}

			Visit<Object> actualVisit = target
					.searchInVisit(visitStart, true);
			assertNotNull(
					String.format("Cannot find the exact visit %d", visitStart),
					actualVisit);
			assertEquals("Wrong visit", visitStart, actualVisit.getKey());

			
			Long prevDocEndTime = visitStart;
			for (File microLoc : visit.listFiles()) {
				Long docEndTime = Long.parseLong(StringUtils.removeLastNChars(
						microLoc.getName(), 5));

				KeyValuePair<Long, Object> expectedDoc = target
						.searchInMicroLocs(actualVisit, docEndTime, true);

				assertNotNull(String.format(
						"Cannot find the exact microloc (%d:%d,%d)",
						visitStart, docEndTime, docEndTime), expectedDoc);

				assertEquals("Wrong Microloc", docEndTime, expectedDoc.getKey());

				long inexactEndTime = docEndTime - Math.round(rand.nextFloat() * (docEndTime - prevDocEndTime - 1));

				Object actualDoc = target.getDocForEndTime(inexactEndTime);
				assertSame("Retrieved a wrong document", expectedDoc.getValue(),
						actualDoc);

				prevDocEndTime = docEndTime;
			}
		}
	}

}
