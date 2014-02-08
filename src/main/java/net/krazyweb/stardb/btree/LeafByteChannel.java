package net.krazyweb.stardb.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockStorage;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class LeafByteChannel extends SeekableInMemoryByteChannel {
	
	private String leafMagic = "LL";
	
	private BlockStorage blockStorage;
	private SeekableInMemoryByteChannel blockBuffer;
	
	protected LeafByteChannel(final BlockStorage blockStorage, final SeekableInMemoryByteChannel blockBuffer) {
		this.blockStorage = blockStorage;
		this.blockBuffer = blockBuffer;
	}

    /**
     * {@inheritDoc}
     *
     * @see java.nio.channels.SeekableByteChannel#read(java.nio.ByteBuffer)
     */
    @Override
    public int read(final ByteBuffer destination) throws IOException {
    	return 0;
    }
	
	public ByteBuffer read(final int size) throws IOException, StarDBException {
		
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
