package net.krazyweb.stardb.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IndexNode {
	
	protected class IndexElement implements Comparable<IndexElement> {
		
		private byte[] key;
		private int pointer;
		
		public IndexElement(final byte[] key, final int pointer) {
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
	
	public IndexNode() {
		selfPointer = 0;
		level = 0;
		beginPointer = 0;
		pointers = new ArrayList<>();
	}
	
	/*
	 * Note: This will not return the location of the key's IndexElement if the key is
     *  not in the index - it'll return the position where it should be inserted. This is
	 * used internally to figure out which branch of the tree to go down.
	 */
	protected int find(final byte[] key) {
		int i = Collections.binarySearch(pointers, new IndexElement(key, 0));
		if (i < 0) {
			i += 1;
			i = Math.abs(i);
		}
		if (i != pointers.size() && Arrays.equals(pointers.get(i).key, key)) {
			System.out.println("OPTION");
			return i + 1;
		} else {
			System.out.println("DECISION");
			return i;
		}
	}
	
	protected int size() {
		if (beginPointer != 0) {
			return pointers.size() + 1;
		} else {
			return 0;
		}
	}
	
	protected int pointer(final int i) {
		if (i == 0) {
			return beginPointer;
		} else {
			return pointers.get(i - 1).pointer;
		}
	}
	
	//Since self.pointers must be sorted, this should be used with care
	protected void addPointer(final byte[] key, final int pointer) {
		pointers.add(new IndexElement(key, pointer));
	}
	
}