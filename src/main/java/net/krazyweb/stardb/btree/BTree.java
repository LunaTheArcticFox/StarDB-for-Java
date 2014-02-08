package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import net.krazyweb.stardb.exceptions.StarDBException;

public abstract class BTree {
	
	protected boolean rootIsLeaf;
	protected int rootPointer;
	
	/**
	 * 
	 */
	protected BTree() {
		rootIsLeaf = false;
		rootPointer = 0;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws StarDBException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	protected byte[] find(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		if (rootIsLeaf) {
			return findInLeaf(loadLeaf(rootPointer), key);
		} else {
			return findInIndex(loadIndex(rootPointer), key);
		}
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws StarDBException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public byte[] getItem(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		return find(key);
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws StarDBException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	protected boolean contains(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		if (find(key) != null) {
			return true;
		} else {
			return false;
		}
	}
	
	private byte[] findInLeaf(final LeafNode leaf, final byte[] key) {
		return leaf.findData(key);
	}
	
	private byte[] findInIndex(final IndexNode index, final byte[] key) throws StarDBException, IOException {
		int i = index.find(key);
		if (index.level == 0) {
			return findInLeaf(loadLeaf(index.pointer(i)), key);
		} else {
			return findInIndex(loadIndex(index.pointer(i)), key);
		}
	}
	
	/**
	 * 
	 * @param pointer
	 * @return
	 * @throws StarDBException
	 * @throws IOException
	 */
	protected abstract LeafNode loadLeaf(final int pointer) throws StarDBException, IOException;
	
	/**
	 * 
	 * @param pointer
	 * @return
	 * @throws StarDBException
	 * @throws IOException
	 */
	protected abstract IndexNode loadIndex(final int pointer) throws StarDBException, IOException;
	
}
