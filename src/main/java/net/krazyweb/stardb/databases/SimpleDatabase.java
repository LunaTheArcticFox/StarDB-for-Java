package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.krazyweb.stardb.btree.BTreeDatabase;
import net.krazyweb.stardb.btree.LeafByteChannel;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class SimpleDatabase extends BTreeDatabase {
	
	private String contentIdentifier;
	private int keySize;
	
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
	protected byte[] readKey(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException {
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
	protected byte[] readData(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException {
		
		int size = readVLQU(buff);
		
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
	
	public int readVLQU(final SeekableInMemoryByteChannel stream) throws IOException, StarDBException {
		
		if (stream instanceof LeafByteChannel) {
			
			int value = 0;
			
			LeafByteChannel leafStream = (LeafByteChannel) stream;
			
			while (true) {
				
				ByteBuffer buff = leafStream.read(1);
				
				int temp = buff.get();
				
				value = (value << 7 | (temp & 0x7f));
				
				if ((temp & 0x80) == 0) {
					break;
				}
				
			}
			
			return value;
			
		} else {
			
			int value = 0;
			
			while (true) {
				
				ByteBuffer buff = ByteBuffer.allocate(1);
				buff.order(ByteOrder.BIG_ENDIAN);
				stream.read(buff);
				buff.rewind();
				
				int temp = buff.get();
				
				value = (value << 7 | (temp & 0x7f));
				
				if ((temp & 0x80) == 0) {
					break;
				}
				
			}
	    		
			return value;
		
		}
		
	}
	
}
