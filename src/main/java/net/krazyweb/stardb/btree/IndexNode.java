package net.krazyweb.stardb.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IndexNode {
	
	private class IndexElement implements Comparable<IndexElement> {
		
		private byte[] key;
		private int pointer;
		
		private IndexElement(final byte[] key, final int pointer) {
			this.key = key;
			this.pointer = pointer;
		}

		@Override
		public int compareTo(final IndexElement other) {
			
			if (Arrays.equals(key, other.key)) {
				return 0;
			}
			
			for (int i = 0; i < key.length; i++) {
				if (key[i] == other.key[i]) {
					continue;
				}
				return (char) key[i] - (char) other.key[i];
			}
			
			return 0;
			
		}
		
		@Override
		public boolean equals(final Object other) {
			if (other instanceof IndexElement) { //TODO Better equals
				IndexElement i = (IndexElement) other;
				return Arrays.equals(key, i.key);
			}
			return false;
		}
		
	}
	
	protected int selfPointer;
	protected char level;
	protected int beginPointer;
	private List<IndexElement> pointers;
	
	/**
	 * Creates a new IndexNode, which contains pointers to other nodes.
	 */
	protected IndexNode() {
		selfPointer = 0;
		level = 0;
		beginPointer = 0;
		pointers = new ArrayList<>();
	}
	
	/**
	 * Finds the index in the list of pointers of the given key.
	 * @param key - The key for which to search in the pointer list.
	 * @return The index of the given key.
	 */
	protected int find(final byte[] key) {
		int i = Collections.binarySearch(pointers, new IndexElement(key, 0));
		if (i < 0) {
			i += 1;
			i = Math.abs(i);
		}
		if (i != pointers.size() && Arrays.equals(pointers.get(i).key, key)) {
			return i + 1;
		} else {
			return i;
		}
	}
	
	/**
	 * Returns the number of pointers within this node.
	 * @return The number of pointers within this node.
	 */
	protected int size() {
		if (beginPointer != 0) {
			return pointers.size() + 1;
		} else {
			return 0;
		}
	}
	
	/**
	 * Retrieves the pointer at the given index.
	 * @param i - The index of the pointer to retrieve.
	 * @return The pointer at the given index.
	 */
	protected int pointer(final int i) {
		if (i == 0) {
			return beginPointer;
		} else {
			return pointers.get(i - 1).pointer;
		}
	}
	
	/**
	 * Adds a pointer to this node's pointers. This should be used with care, as the list *must* remain sorted.
	 * @param key - The key of the pointer so that it can be retrieved.
	 * @param pointer - The pointer itself.
	 */
	protected void addPointer(final byte[] key, final int pointer) {
		pointers.add(new IndexElement(key, pointer));
	}
	
}