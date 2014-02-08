package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockStorage;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public abstract class BTreeDatabase extends BTree {
	
	private String fileIdentifier = "BTreeDB4";
	private String indexMagic = "II";
	private String leafMagic = "LL";
	
	private BlockStorage blockStorage;
	private Map<Integer, IndexNode> indexCache;
	
	protected BTreeDatabase(final BlockStorage blockStorage) {
		super();
		this.blockStorage = blockStorage;
		this.indexCache = new HashMap<>();
	}
	
	protected void readRoot() throws StarDBException, IOException {
		
		SeekableInMemoryByteChannel rootData = blockStorage.readUserData(28, 14);
		
		ByteBuffer buffer = ByteBuffer.allocate(1);
		rootData.read(buffer);
		buffer.rewind();
		
		boolean unknownBool = (buffer.get() == 1);
		
		rootData.position(rootData.position() + 1);
		
		if (unknownBool) {
			rootData.position(rootData.position() + 8);
		}
		
		buffer = ByteBuffer.allocate(5);
		buffer.order(ByteOrder.BIG_ENDIAN);
		rootData.read(buffer);
		buffer.rewind();
		
		rootPointer = buffer.getInt();
		rootIsLeaf = (buffer.get() == 1);
		
	}
	
	protected void open() throws StarDBException, IOException {
		
		blockStorage.open();

		SeekableInMemoryByteChannel userData = blockStorage.readUserData(0, 28);

		ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.order(ByteOrder.BIG_ENDIAN);
		userData.read(buffer);
		buffer.rewind();
		
		String fileID = new String(buffer.array());
		
		if (!fileID.startsWith(fileIdentifier)) {
			throw new StarDBException("DB file identifier does not match expected value of " + fileIdentifier + " (Got " + fileID + ")");
		}
		
		buffer = ByteBuffer.allocate(12);
		buffer.order(ByteOrder.BIG_ENDIAN);
		userData.read(buffer);
		buffer.rewind();
		
		String contentID = new String(buffer.array());
		
		if (!contentID.startsWith(getContentIdentifier())) {
			throw new StarDBException("DB content identifier does not match expected value of " + getContentIdentifier() + " (Got " + contentID + ")");
		}
		
		buffer = ByteBuffer.allocate(4);
		buffer.order(ByteOrder.BIG_ENDIAN);
		userData.read(buffer);
		buffer.rewind();
		
		int keySize = buffer.getInt();
		
		if (keySize != getKeySize()) {
			throw new StarDBException("DB content key size does not match expected value of " + getKeySize() + " (Got " + keySize + ")");
		}
		
		readRoot();
		
	}
	
	protected IndexNode readIndex(final int pointer) throws StarDBException, IOException {
		
		IndexNode index = new IndexNode();

		SeekableInMemoryByteChannel buff = blockStorage.readBlock(pointer);
		
		ByteBuffer buffer = ByteBuffer.allocate(2);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buff.read(buffer);
		buffer.rewind();
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(indexMagic)) {
			throw new StarDBException("Incorrect index block signature.");
		}

		buffer = ByteBuffer.allocate(9);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buff.read(buffer);
		buffer.rewind();
		
		index.selfPointer = pointer;
		index.level = (char) buffer.get();
		
		int numChildren = buffer.getInt();
		index.beginPointer = buffer.getInt();
		
		for (int i = 0; i < numChildren; i++) {
			
			byte[] key = readKey(buff);

			buffer = ByteBuffer.allocate(4);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buff.read(buffer);
			buffer.rewind();
			int newPointer = buffer.getInt();
			
			index.addPointer(key, newPointer);
			
		}
		
		return index;
		
	}

	@Override
	protected IndexNode loadIndex(int pointer) throws StarDBException, IOException {
		
		if (!indexCache.containsKey(pointer)) {
			IndexNode index = readIndex(pointer);
			indexCache.put(pointer, index);
			return index;
		} else {
			return indexCache.get(pointer);
		}
		
	}

	@Override
	protected LeafNode loadLeaf(int pointer) throws StarDBException, IOException {
		
		LeafNode leaf = new LeafNode();
		
		SeekableInMemoryByteChannel buff = blockStorage.readBlock(pointer);

		ByteBuffer buffer = ByteBuffer.allocate(2);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buff.read(buffer);
		buffer.rewind();
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(leafMagic)) {
			throw new StarDBException("Incorrect leaf block signature.");
		}

		leaf.selfPointer = pointer;
		
		LeafByteChannel leafInput = new LeafByteChannel(blockStorage, buff);
		
		int count = leafInput.read(4).getInt();
		
		for (int i = 0; i < count; i++) {
			byte[] key = readKey(leafInput);
			byte[] data = readData(leafInput);
			leaf.addElement(key, data);
		}
		
		return leaf;
		
	}
	
	protected abstract int getKeySize();
	protected abstract String getContentIdentifier();
	protected abstract byte[] readKey(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException;
	protected abstract byte[] readData(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException;
	
}