package net.krazyweb.stardb.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.krazyweb.stardb.exceptions.StarDBException;

public class LeafNode {
	
	protected class LeafElement implements Comparable<LeafElement> {
		
		private byte[] key;
		private byte[] data;
		
		private LeafElement(final byte[] key, final byte[] data) {
			this.key = key;
			this.data = data;
		}

		@Override
		public int compareTo(final LeafElement other) {
			
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
		
	}
	
	protected int selfPointer;
	protected int nextLeaf;
	protected List<LeafElement> elements;

	/**
	 * Creates a new LeafNode, which contains data at the end of the tree.
	 */
	protected LeafNode() {
		selfPointer = 0;
		nextLeaf = 0;
		elements = new ArrayList<>(); //This list must be sorted by key
	}

	/**
	 * Finds the data for the given key.
	 * @param key - The key for the data.
	 * @return The data for the given key as a byte array.
	 */
	protected byte[] findData(final byte[] key) {
		int i = Collections.binarySearch(elements, new LeafElement(key, new byte[] {}));
		if (i < 0) {
			i += 1;
			i = Math.abs(i);
		}
		if (i != elements.size() && Arrays.equals(elements.get(i).key, key)) {
			return elements.get(i).data;
		} else {
			return null;
		}
	}
	
	/**
	 * Returns the data for the given key.
	 * @param key - The key for the data.
	 * @return The data for the given key as a byte array.
	 * @throws StarDBException The key could not be found in the database.
	 */
	protected byte[] getItem(final byte[] key) throws StarDBException {
		return findData(key);
	}
	
	/**
	 * Adds data to this node for the specified key. This should be used with care, as the list *must* remain sorted.
	 * @param key - The key of the data so that it can be retrieved.
	 * @param data - The data itself.
	 */
	protected void addElement(final byte[] key, final byte[] data) {
		elements.add(new LeafElement(key, data));
	}
	
}