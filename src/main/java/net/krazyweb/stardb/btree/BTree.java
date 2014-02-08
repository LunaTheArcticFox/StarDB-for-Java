package net.krazyweb.stardb.btree;

import net.krazyweb.stardb.exceptions.StarDBException;

public abstract class BTree {
	
	protected boolean rootIsLeaf;
	protected int rootPointer;
	
	/**
	 * Creates a new BTree instance used as the base for all database operations.
	 */
	protected BTree() {
		rootIsLeaf = false;
		rootPointer = 0;
	}
	
	/**
	 * Retrieves the data for the specified key from the database.
	 * @param key - The key to retrieve the data for.
	 * @return The data for the specified key as a byte array.
	 * @throws StarDBException An error occurred while retrieving the data for the specified key.
	 */
	protected byte[] find(final byte[] key) throws StarDBException {
		if (rootIsLeaf) {
			return findInLeaf(loadLeaf(rootPointer), key);
		} else {
			return findInIndex(loadIndex(rootPointer), key);
		}
	}
	
	/**
	 * Retrieves the data for the specified key from the database.
	 * @param key - The key to retrieve the data for.
	 * @return The data for the specified key as a byte array or null if the key is not found.
	 * @throws StarDBException An error occurred while retrieving the data for the specified key.
	 */
	protected byte[] getItem(final byte[] key) throws StarDBException {
		return find(key);
	}
	
	/**
	 * Checks to see if the database contains the specified key.
	 * @param key - The key to search for.
	 * @return Whether or not the key is found.
	 * @throws StarDBException An error occurred while retrieving the data for the specified key.
	 */
	protected boolean contains(final byte[] key) throws StarDBException {
		if (find(key) != null) {
			return true;
		} else {
			return false;
		}
	}
	
	private byte[] findInLeaf(final LeafNode leaf, final byte[] key) throws StarDBException {
		return leaf.findData(key);
	}
	
	private byte[] findInIndex(final IndexNode index, final byte[] key) throws StarDBException {
		int i = index.find(key);
		if (index.level == 0) {
			return findInLeaf(loadLeaf(index.pointer(i)), key);
		} else {
			return findInIndex(loadIndex(index.pointer(i)), key);
		}
	}
	
	/**
	 * Reads and parses the leaf at the specified pointer.
	 * @param pointer - The block index of the leaf in the database.
	 * @return The LeafNode at the specified pointer.
	 * @throws StarDBException An error occurred while reading the data for the specified leaf.
	 */
	protected abstract LeafNode loadLeaf(final int pointer) throws StarDBException;
	
	/**
	 * Reads and parses the index at the specified pointer.
	 * @param pointer - The block index of the index in the database.
	 * @return The IndexNode at the specified pointer.
	 * @throws StarDBException An error occurred while reading the data for the specified index.
	 */
	protected abstract IndexNode loadIndex(final int pointer) throws StarDBException;
	
}
