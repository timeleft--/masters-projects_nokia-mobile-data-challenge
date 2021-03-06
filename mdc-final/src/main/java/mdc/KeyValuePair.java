package mdc;

import java.util.Map;

public class KeyValuePair<K, V> implements Map.Entry<K, V> {
	protected K key;
	protected V value;

	public KeyValuePair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public KeyValuePair() {
		// TODO Auto-generated constructor stub
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public V setValue(V value) {
		this.value = value;
		return value;
	}

	@Override
	public String toString() {

		return key.toString() + ": " + value.toString();
	}
}