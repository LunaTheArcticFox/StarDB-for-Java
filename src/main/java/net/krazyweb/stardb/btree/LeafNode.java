package net.krazyweb.stardb.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LeafNode {
	
	protected class LeafElement implements Comparable<LeafElement> {
		
		private byte[] key;
		private Object data;
		
		public LeafElement(final byte[] key, final Object data) {
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
				return key[i] - other.key[i];
			}
			
			return 0;
			
		}
		
	}
	
	protected int selfPointer;
	protected int nextLeaf;
	protected List<LeafElement> elements;
	
	public LeafNode() {
		selfPointer = 0;
		nextLeaf = 0;
		elements = new ArrayList<>(); //This list must be sorted by key
	}
	
	protected Object findData(final byte[] key) {
		int i = Collections.binarySearch(elements, new LeafElement(key, 0));
		if (i < 0) {
			i += 1;
			i = Math.abs(i);
		}
		if (i != elements.size() && elements.get(i).key.equals(key)) {
			return elements.get(i).data;
		} else {
			return null;
		}
	}
	
	protected Object getItem(final byte[] key) {
		return findData(key);
	}
	
	//Since self.elements must be sorted, this should be used with care
	protected void addElement(final byte[] key, final Object data) {
		elements.add(new LeafElement(key, data));
	}
	
}