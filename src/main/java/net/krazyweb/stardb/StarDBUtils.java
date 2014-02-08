package net.krazyweb.stardb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.exceptions.StarDBException;

public class StarDBUtils {
	
	/**
	 * Reads some kind of size data from the file. The original implementation of this method in Python did not explain the acronym.
	 * @param byteChannel - The SeekableByteChannel to read from.
	 * @return - A value of some kind, seemingly used to specify object data lengths.
	 * @throws StarDBException
	 */
	public static int readVLQU(final SeekableByteChannel byteChannel) throws StarDBException {
		
		int value = 0;
		
		ByteBuffer buff = readToBuffer(byteChannel, 1);
		
		while (true) {
			
			int temp = buff.get();
			
			value = (value << 7 | (temp & 0x7f));
			
			if ((temp & 0x80) == 0) {
				break;
			}
			
		}
    		
		return value;
		
	}
	
	/**
	 * Reads a series of bytes from a SeekableByteChannel into a new ByteBuffer of the specified length.
	 * @param byteChannel - The SeekableByteChannel from which to read the data.
	 * @param size - The amount of data to read in bytes.
	 * @return - A ByteBuffer containing the requested data.
	 * @throws StarDBException An error occurred while trying to read the ByteChannel.
	 */
	public static ByteBuffer readToBuffer(final SeekableByteChannel byteChannel, final int size) throws StarDBException {

		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.order(ByteOrder.BIG_ENDIAN);
		try {
			byteChannel.read(buffer);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		buffer.flip();
		
		return buffer;
		
	}

}
