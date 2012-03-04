package uwaterloo.mdc.etl.model;

import java.util.ArrayList;

import uwaterloo.mdc.etl.util.KeyValuePair;

public class Visit<V> extends
		KeyValuePair<Long, ArrayList<KeyValuePair<Long, V>>> {
	public final Character trust;
	public Visit(Long key, ArrayList<KeyValuePair<Long, V>> value, Character trust) {
		super(key, value);
		this.trust = trust;
	}

}
