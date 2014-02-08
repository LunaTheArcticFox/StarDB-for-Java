package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Map;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockStorage;

public abstract class BTreeDatabase extends BTree {
	
	private String fileIdentifier = "BTreeDB4";
	private String indexMagic = "II";
	private String leafMagic = "LL";
	
	private BlockStorage blockStorage;
	private Map<Integer, IndexNode> indexCache;
	
	/**
	 * Constructs a new BTreeDatabase object that retrieves its data from the BlockStorage object. 
	 * @param blockStorage - The object containing the b-tree file.
	 */
	protected BTreeDatabase(final BlockStorage blockStorage) {
		super();
		this.blockStorage = blockStorage;
		this.indexCache = new HashMap<>();
	}
	
	/**
	 * Reads the data from the root node from which the tree can be searched.
	 * @throws StarDBException - An error occurred while reading the root data.
	 */
	protected void readRoot() throws StarDBException {
		
		SeekableByteChannel rootData = blockStorage.readUserData(28, 14);
		
		ByteBuffer buffer = StarDBUtils.readToBuffer(rootData, 1);
		
		boolean unknownBool = (buffer.get() == 1);
		
		try {
			rootData.position(rootData.position() + 1);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		if (unknownBool) {
			try {
				rootData.position(rootData.position() + 8);
			} catch (IOException e) {
				throw new StarDBException("Error: " + e.getMessage(), e);
			}
		}
		
		buffer = StarDBUtils.readToBuffer(rootData, 5);
		
		rootPointer = buffer.getInt();
		rootIsLeaf = (buffer.get() == 1);
		
	}
	
	/**
	 * Opens the database file for reading and verifies its type.
	 * @throws StarDBException - The file is not a valid database file or an error occurred while reading the file.
	 */
	protected void open() throws StarDBException {
		
		blockStorage.open();
		
		SeekableByteChannel userData = blockStorage.readUserData(0, 28);
		
		ByteBuffer buffer = StarDBUtils.readToBuffer(userData, 12);
		
		String fileID = new String(buffer.array());
		
		if (!fileID.startsWith(fileIdentifier)) {
			throw new StarDBException("DB file identifier does not match expected value of " + fileIdentifier + " (Got " + fileID + ")");
		}
		
		buffer = StarDBUtils.readToBuffer(userData, 12);
		
		String contentID = new String(buffer.array());
		
		if (!contentID.startsWith(getContentIdentifier())) {
			throw new StarDBException("DB content identifier does not match expected value of " + getContentIdentifier() + " (Got " + contentID + ")");
		}
		
		buffer = StarDBUtils.readToBuffer(userData, 4);
		
		int keySize = buffer.getInt();
		
		if (keySize != getKeySize()) {
			throw new StarDBException("DB content key size does not match expected value of " + getKeySize() + " (Got " + keySize + ")");
		}
		
		readRoot();
		
	}
	
	/**
	 * Reads the index block at the specified pointer and parses its contents into nodes.
	 * @param pointer - The index of the block in the database file.
	 * @return The node containing this block's, well, nodes.
	 * @throws StarDBException - The block is not an index block or an error occurred while reading the block data.
	 */
	protected IndexNode readIndex(final int pointer) throws StarDBException {
		
		IndexNode index = new IndexNode();

		SeekableByteChannel byteChannel = blockStorage.readBlock(pointer);
		
		ByteBuffer buffer = StarDBUtils.readToBuffer(byteChannel, 2);
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(indexMagic)) {
			throw new StarDBException("Incorrect index block signature.");
		}

		buffer = StarDBUtils.readToBuffer(byteChannel, 9);
		
		index.selfPointer = pointer;
		index.level = (char) buffer.get();
		
		int numChildren = buffer.getInt();
		index.beginPointer = buffer.getInt();
		
		for (int i = 0; i < numChildren; i++) {
			
			byte[] key = readKey(byteChannel);
			
			buffer = StarDBUtils.readToBuffer(byteChannel, 4);
			int newPointer = buffer.getInt();
			
			index.addPointer(key, newPointer);
			
		}
		
		return index;
		
	}

	@Override
	protected IndexNode loadIndex(int pointer) throws StarDBException {
		
		if (!indexCache.containsKey(pointer)) {
			IndexNode index = readIndex(pointer);
			indexCache.put(pointer, index);
			return index;
		} else {
			return indexCache.get(pointer);
		}
		
	}

	@Override
	protected LeafNode loadLeaf(int pointer) throws StarDBException {
		
		LeafNode leaf = new LeafNode();
		
		SeekableByteChannel byteChannel = blockStorage.readBlock(pointer);

		ByteBuffer buffer = StarDBUtils.readToBuffer(byteChannel, 2);
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(leafMagic)) {
			throw new StarDBException("Incorrect leaf block signature.");
		}

		leaf.selfPointer = pointer;
		
		SeekableByteChannel leafInput = new LeafByteChannel(blockStorage, byteChannel);
		
		buffer = StarDBUtils.readToBuffer(leafInput, 4);
		
		int count = buffer.getInt();
		
		for (int i = 0; i < count; i++) {
			byte[] key = readKey(leafInput);
			byte[] data = readData(leafInput);
			leaf.addElement(key, data);
		}
		
		return leaf;
		
	}
	
	/**
	 * Returns the size of the key value.
	 * @return The size of the key value in bytes.
	 */
	protected abstract int getKeySize();
	
	/**
	 * Returns the content identifier.
	 * @return The content identifier used to verify the database's contents.
	 */
	protected abstract String getContentIdentifier();
	
	/**
	 * Reads the key at the current position in the byteChannel.
	 * @param byteChannel - The ByteChannel from which to read the key.
	 * @return The key as a byte array.
	 * @throws StarDBException - An error occurred while attempting to read the key.
	 */
	protected abstract byte[] readKey(final SeekableByteChannel byteChannel) throws StarDBException;
	
	/**
	 * Reads the data stored at the current position in the byteChannel.
	 * @param byteChannel - The ByteChannel from which to read the stored data.
	 * @return The stored data as a byte array.
	 * @throws StarDBException - An error occurred while attempting to read the data.
	 */
	protected abstract byte[] readData(final SeekableByteChannel byteChannel) throws StarDBException;
	
}