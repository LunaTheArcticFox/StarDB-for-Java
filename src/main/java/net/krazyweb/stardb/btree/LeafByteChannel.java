package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.SeekableInMemoryByteChannel;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockStorage;

public class LeafByteChannel extends SeekableInMemoryByteChannel {
	
	private String leafMagic = "LL";
	
	private BlockStorage blockStorage;
	private SeekableByteChannel blockBuffer;
	
	protected LeafByteChannel(final BlockStorage blockStorage, final SeekableByteChannel blockBuffer) {
		this.blockStorage = blockStorage;
		this.blockBuffer = blockBuffer;
		open = true;
	}
	
    @Override
    public int read(final ByteBuffer destination) throws IOException {
    	
    	final int size = destination.capacity();
    	
		ByteBuffer data = ByteBuffer.allocate(size);
		
		long blockDataSize = blockStorage.getBlockSize() - 4;
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
                    try {
						blockBuffer = blockStorage.readBlock(nextBlockPointer);
					} catch (StarDBException e) {
						throw new IOException("Error: " + e.getMessage(), e);
					}
                    tempBuffer = ByteBuffer.allocate(2);
                    tempBuffer.order(ByteOrder.BIG_ENDIAN);
                    blockBuffer.read(tempBuffer);
                    tempBuffer.rewind();
                    String magic = new String(tempBuffer.array());
                    if (!magic.equals(leafMagic)) {
	                    throw new IOException("Incorrect leaf block signature"); //RUNTIMEEXCEPTION
                    }
                } else { 
                    throw new IOException("Insufficient leaf data"); //RUNTIMEEXCEPTION
                }
			}

		}

		data.rewind();
		destination.put(data);
		destination.rewind();
		
    	return 0;
    	
    }

}
