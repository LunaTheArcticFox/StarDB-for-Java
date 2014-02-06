package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

import net.krazyweb.stardb.btree.BTreeDatabase;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class SimpleDatabase extends BTreeDatabase {
	
	private String contentIdentifier;
	private int keySize;
	
	public SimpleDatabase(final BlockFile blockFile, final String contentID, final int keySize) {
		super(blockFile);
		contentIdentifier = contentID;
		this.keySize = keySize;
	}

	@Override
	public int getKeySize() {
		return keySize;
	}

	@Override
	public String getContentIdentifier() {
		return contentIdentifier;
	}

	@Override
	public byte[] readKey(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException {
		if (buff instanceof LeafInputStream) {
			ByteBuffer buffer = ((LeafInputStream) buff).read(keySize);
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
	public Object readData(final SeekableInMemoryByteChannel buff) throws IOException, StarDBException {
		
		int size = readVLQU(buff);
		
		if (buff instanceof LeafInputStream) {
			
			ByteBuffer buffer = ((LeafInputStream) buff).read(size);
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
		
		if (stream instanceof LeafInputStream) {
			
			int value = 0;
			
			LeafInputStream leafStream = (LeafInputStream) stream;
			
			while (true) {
				
				ByteBuffer buff = leafStream.read(1);
				
				byte temp = buff.get();
				
				value = (value << 7 | (temp & 0x7f));
				
				if ((temp & 0x80) == 0) {
					break;
				}
				
			}
			
			ByteBuffer buff = leafStream.read(value);
	    	
			return buff.getInt();
			
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
			
			ByteBuffer buff = ByteBuffer.allocate(value);
			buff.order(ByteOrder.BIG_ENDIAN);
			stream.read(buff);
			buff.rewind();
	    		
			return buff.getInt();
		
		}
		
	}
	
}
