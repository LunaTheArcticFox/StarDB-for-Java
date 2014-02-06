package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import net.krazyweb.stardb.exceptions.StarDBException;

public abstract class BTree {
	
	protected boolean rootIsLeaf;
	protected int rootPointer;
	
	public BTree() {
		rootIsLeaf = false;
		rootPointer = 0;
	}
	
	public Object find(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		if (rootIsLeaf) {
			return findInLeaf(loadLeaf(rootPointer), key);
		} else {
			return findInIndex(loadIndex(rootPointer), key);
		}
	}
	
	public Object getItem(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		return find(key);
	}
	
	public boolean contains(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		if (find(key) != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public Object findInLeaf(final LeafNode leaf, final byte[] key) {
		return leaf.findData(key);
	}
	
	public Object findInIndex(final IndexNode index, final byte[] key) throws StarDBException, IOException {
		int i = index.find(key);
		System.out.println(i);
		if (index.level == 0) {
			return findInLeaf(loadLeaf(index.pointer(i)), key);
		} else {
			return findInIndex(loadIndex(index.pointer(i)), key);
		}
	}
	
	public Set<Object> getAllValues() throws StarDBException, IOException {
		Set<Object> output = new HashSet<>();
		if (!rootIsLeaf) {
			getAllValuesFromIndex(loadIndex(rootPointer), output);
		} else {
			getAllValuesFromLeaf(loadLeaf(rootPointer), output);
		}
		return output;
	}
	
	public void getAllValuesFromIndex(final IndexNode index, final Set<Object> output) throws StarDBException, IOException {
		for (int i = 0; i < index.size(); i++) {
			if (index.level != 0) {
				getAllValuesFromIndex(loadIndex(index.pointer(i)), output);
			} else {
				getAllValuesFromLeaf(loadLeaf(index.pointer(i)), output);
			}
		}
	}
	
	public void getAllValuesFromLeaf(final LeafNode leaf, final Set<Object> output) {
		for (Object element : leaf.elements) {
			output.add(element);
		}
	}
	
	public abstract LeafNode loadLeaf(final int pointer) throws StarDBException, IOException;
	public abstract IndexNode loadIndex(final int pointer) throws StarDBException, IOException;
	
	/*

	    def getAllValuesFromLeaf(self, leaf):
	        for element in leaf.elements:
	            yield element.key, element.data

	    def loadLeaf(self, pointer):
	        raise NotImplementedError

	    def loadIndex(self, pointer):
	        raise NotImplementedError*/
}
