package uwaterloo.mdc.etl.model;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;

import uwaterloo.mdc.etl.Discretize;
import uwaterloo.mdc.etl.util.KeyValuePair;
import uwaterloo.mdc.etl.util.StringUtils;

public class UserVisitsDocsHierarchy<V> {

	protected final File userDir;
	protected final ArrayList<KeyValuePair<Long, ArrayList<KeyValuePair<Long, V>>>> visitsList;
	protected final ArrayList<Character> visitTrust;
	protected final HashMap<Long, ArrayList<Character>> microLocTrustMap;

	public UserVisitsDocsHierarchy(File userDir, Constructor<V> vConstr)
			throws Exception {
		super();
		this.userDir = userDir;
		visitsList = new ArrayList<KeyValuePair<Long, ArrayList<KeyValuePair<Long, V>>>>();
		visitTrust = new ArrayList<Character>();
		microLocTrustMap = new HashMap<Long, ArrayList<Character>>();

		File[] visitDirArr = userDir.listFiles();
		for (File visitDir : visitDirArr) {
			if (!visitDir.isDirectory()) {
				continue;
			}
			Long visitStartTime = Long.parseLong(StringUtils.removeLastNChars(
					visitDir.getName(), 1));

			Visit<V> microLocsList = new Visit<V>(visitStartTime,
					new ArrayList<KeyValuePair<Long, V>>(),
					StringUtils.charAtFromEnd(visitDir.getName(), 1));
			ArrayList<Character> microLocTrust = new ArrayList<Character>();

			File[] visitEndFiles = visitDir.listFiles();

			for (int j = visitEndFiles.length - 1; j >= 0; --j) {
				// Descending traversal
				microLocsList.getValue().add(
						new KeyValuePair<Long, V>(
								Long.parseLong(StringUtils.removeLastNChars(
										visitEndFiles[j].getName(), 5)),
								getValueObj(vConstr, visitEndFiles[j])));
				microLocTrust.add(StringUtils.charAtFromEnd(
						visitEndFiles[j].getName(), 5));
			}
			if (microLocsList.getValue().size() == 0) {
				// No files inside any more.. just a sanity check!
				// Because the data is insane: user 039 has visits that start
				// and end in different timezones
				// Actually too many to be those that started just before the
				// day light saving time
				continue;
			}
			visitsList.add(microLocsList);
			 visitTrust.add(StringUtils.charAtFromEnd(visitDir.getName(), 1));

			microLocTrustMap.put(visitStartTime, microLocTrust);

		}

	}

	protected V getValueObj(Constructor<V> vConstr, File visitEndFile)
			throws Exception {
		return vConstr.newInstance();
	}

	public ArrayList<KeyValuePair<Long, V>> getVisitForTime(long startTimeInSecs) {
		Visit<V> visit = searchInVisit(startTimeInSecs, false);
		if (visit == null) {
			return null;
		} else {
			return visit.getValue();
		}
	}

	public V getDocForEndTime(long endTimeInSecs) {
		Visit<V> visit = searchInVisit(endTimeInSecs, false);
		if (visit == null) {
			return null;
		}

		KeyValuePair<Long, V> microLoc = searchInMicroLocs(visit,
				endTimeInSecs, false);
		if (microLoc != null) {
			return microLoc.getValue();
		} else {
			return null;
		}

	}

	public V getDocExact(Long visitStartTime, Long docEndTime) {
		Visit<V> visit = searchInVisit(visitStartTime, true);
		if (visit == null) {
			return null;
		}
		KeyValuePair<Long, V> microLoc = searchInMicroLocs(visit, docEndTime,
				true);
		if (microLoc != null) {
			return microLoc.getValue();
		} else {
			return null;
		}
	}

	public Visit<V> searchInVisit(Long startTimeInSecs, boolean exact) {
		TimeSearch<ArrayList<KeyValuePair<Long, V>>> visitSearch = new TimeSearch<ArrayList<KeyValuePair<Long, V>>>();
		visitSearch.setExact(exact);
		Visit<V> visit = (Visit<V>) visitSearch.findForTime(visitsList,
				startTimeInSecs, /* true, */true, visitTrust);
		if (visit == null && !exact) {
			// We first try to find a document without approximation
			// then we resort to approximation in second pass
			boolean secondPass = false;
			do {
				for (int i = visitSearch.prevStart; i <= visitSearch.prevEnd; ++i) {
					Visit<V> visitI = (Visit<V>) visitsList.get(i);
					long visistIStartTime = visitI.getKey();
					long visitIEndTime = visitI.getValue().get(0).getKey();
					if (secondPass) {
						visistIStartTime -= Discretize
								.getStartEndTimeError(visitTrust.get(i));
						visitIEndTime += Discretize
								.getStartEndTimeError(microLocTrustMap.get(
										visitI.getKey()).get(0));
					}
					if (visistIStartTime <= startTimeInSecs) {
						if (visitIEndTime >= startTimeInSecs) {
							visit = visitI;
						}
					} else {
						break;
					}
				}
				if (visit != null || secondPass) {
					break;
				} else {
					secondPass = true;
				}
			} while (true);
		}
		return visit;
	}

	public KeyValuePair<Long, V> searchInMicroLocs(Visit<V> visit,
			Long endTimeInSecs, boolean exact) {
		TimeSearch<V> microLocSearch = new TimeSearch<V>();
		microLocSearch.setExact(exact);
		ArrayList<Character> microLocTrust = microLocTrustMap.get(visit
				.getKey());
		KeyValuePair<Long, V> microLoc = microLocSearch.findForTime(
				visit.getValue(), endTimeInSecs, /* false, */false,
				microLocTrust);
		if (microLoc == null && !exact) {
			// We first try to find a document without approximation
			// then we resort to approximation in second pass
			boolean secondPass = false;
			do {
				for (int i = microLocSearch.prevStart; i <= microLocSearch.prevEnd; ++i) {
					KeyValuePair<Long, V> microLocI = visit.getValue().get(i);
					long currTime = microLocI.getKey();
					if (secondPass) {
						currTime += Discretize
								.getStartEndTimeError(microLocTrust.get(i));
					}
					if (currTime >= endTimeInSecs) {
						microLoc = microLocI;
					} else {
						break;
					}
				}

				if (microLoc != null || secondPass) {
					break;
				} else {
					secondPass = true;
				}
			} while (true);
		}
		return microLoc;
	}

}
