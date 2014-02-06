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
	
	public class LeafInputStream extends SeekableInMemoryByteChannel {
		
		private BlockStorage blockStorage;
		private SeekableInMemoryByteChannel blockBuffer;
		
		private LeafInputStream(final BlockStorage blockStorage, final SeekableInMemoryByteChannel blockBuffer) {
			this.blockStorage = blockStorage;
			this.blockBuffer = blockBuffer;
		}
		
		public ByteBuffer read(final int size) throws IOException, StarDBException {
			
			ByteBuffer data = ByteBuffer.allocate(size);
			
			long blockDataSize = blockStorage.blockSize - 4;
			long bytesToRead = size;
			
			while (bytesToRead > 0) {
				
				boolean endOfBlock = false;
				
				if ((blockBuffer.position() + bytesToRead) < blockDataSize) {
					blockBuffer.read(data);
	                bytesToRead = 0;
				} else {
	                long bytesAvailable = blockDataSize - blockBuffer.position();
	                ByteBuffer tempBuffer = ByteBuffer.allocate((int) bytesAvailable);
                    blockBuffer.read(tempBuffer);
                    tempBuffer.rewind();
                    data.put(tempBuffer);
                    bytesToRead -= bytesAvailable;
                    endOfBlock = true;
				}
				
				if (endOfBlock && bytesToRead > 0) {
	                ByteBuffer tempBuffer = ByteBuffer.allocate(4);
	                tempBuffer.order(ByteOrder.BIG_ENDIAN);
	                blockBuffer.read(tempBuffer);
	                tempBuffer.rewind();
	                int nextBlockPointer = tempBuffer.getInt();
	                if (nextBlockPointer != -1) {
	                    blockBuffer = blockStorage.readBlock(nextBlockPointer);
	                    tempBuffer = ByteBuffer.allocate(2);
	                    tempBuffer.order(ByteOrder.BIG_ENDIAN);
	                    blockBuffer.read(tempBuffer);
	                    tempBuffer.rewind();
	                    String magic = new String(tempBuffer.array());
	                    if (!magic.equals(leafMagic)) {
		                    throw new StarDBException("Incorrect leaf block signature");
	                    }
	                } else {
	                    throw new StarDBException("Insufficient leaf data");
	                }
				}

			}

			data.rewind();
			return data;
			
		}
		
	}
	
	public String fileIdentifier = "BTreeDB4";
	public String indexMagic = "II";
	public String leafMagic = "LL";
	
	public BlockStorage blockStorage;
	public Map<Integer, IndexNode> indexCache;
	
	public SeekableInMemoryByteChannel currentBuffer;
	public int currentLeafBlock;
	
	public BTreeDatabase(final BlockStorage blockStorage) {
		super();
		this.blockStorage = blockStorage;
		this.indexCache = new HashMap<>();
	}
	
	public void readRoot() throws StarDBException, IOException {
		
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
	
	public void open() throws StarDBException, IOException {
		
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
	
	public IndexNode readIndex(final int pointer) throws StarDBException, IOException {
		
		IndexNode index = new IndexNode();

		SeekableInMemoryByteChannel buff = blockStorage.readBlock(pointer);
		currentBuffer = buff;
		
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
	public IndexNode loadIndex(int pointer) throws StarDBException, IOException {
		
		if (!indexCache.containsKey(pointer)) {
			IndexNode index = readIndex(pointer);
			indexCache.put(pointer, index);
			return index;
		} else {
			return indexCache.get(pointer);
		}
		
	}

	@Override
	public LeafNode loadLeaf(int pointer) throws StarDBException, IOException {
		
		LeafNode leaf = new LeafNode();
		
		currentLeafBlock = pointer;

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
		
		LeafInputStream leafInput = new LeafInputStream(blockStorage, buff);
		
		int count = leafInput.read(4).getInt();
		
		for (int i = 0; i < count; i++) {
			byte[] key = readKey(leafInput);
			byte[] data = readData(leafInput);
			leaf.addElement(key, data);
		}
		
		return leaf;
		
	}
	
	public abstract int getKeySize();
	public abstract String getContentIdentifier();
	public abstract byte[] readKey(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException;
	public abstract byte[] readData(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException;
	
}