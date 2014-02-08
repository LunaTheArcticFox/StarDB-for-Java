package net.krazyweb.stardb.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
	 * 
	 */
	protected LeafNode() {
		selfPointer = 0;
		nextLeaf = 0;
		elements = new ArrayList<>(); //This list must be sorted by key
	}
	
	/**
	 * 
	 * @param key
	 * @return
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
			System.out.println("Key not found! " + new String(key));
			return null;
		}
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	protected byte[] getItem(final byte[] key) {
		return findData(key);
	}
	
	//Since self.elements must be sorted, this should be used with care
	/**
	 * 
	 * @param key
	 * @param data
	 */
	protected void addElement(final byte[] key, final byte[] data) {
		elements.add(new LeafElement(key, data));
	}
	
}