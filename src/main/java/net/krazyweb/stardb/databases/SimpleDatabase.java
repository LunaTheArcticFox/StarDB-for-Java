package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.btree.BTreeDatabase;
import net.krazyweb.stardb.btree.LeafByteChannel;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class SimpleDatabase extends BTreeDatabase {
	
	private String contentIdentifier;
	private int keySize;
	
	/**
	 * 
	 * @param blockFile
	 * @param contentID
	 * @param keySize
	 */
	protected SimpleDatabase(final BlockFile blockFile, final String contentID, final int keySize) {
		super(blockFile);
		contentIdentifier = contentID;
		this.keySize = keySize;
	}

	@Override
	protected int getKeySize() {
		return keySize;
	}

	@Override
	protected String getContentIdentifier() {
		return contentIdentifier;
	}

	@Override
	protected byte[] readKey(final SeekableByteChannel buff) throws IOException, StarDBException {
		if (buff instanceof LeafByteChannel) {
			ByteBuffer buffer = ((LeafByteChannel) buff).read(keySize);
			return buffer.array();
		} else {
			ByteBuffer buffer = ByteBuffer.allocate(keySize);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buff.read(buffer);
			buffer.rewind();
			return buffer.array();
		}
	}

	@Override
	protected byte[] readData(final SeekableByteChannel buff) throws IOException, StarDBException {
		
		int size = StarDBUtils.readVLQU(buff);
		
		if (buff instanceof LeafByteChannel) {
			
			ByteBuffer buffer = ((LeafByteChannel) buff).read(size);
			return buffer.array();
			
		} else {
			
			ByteBuffer buffer = ByteBuffer.allocate(size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buff.read(buffer);
			buffer.rewind();
			return buffer.array();
			
		}
    	
		
	}
	
}
