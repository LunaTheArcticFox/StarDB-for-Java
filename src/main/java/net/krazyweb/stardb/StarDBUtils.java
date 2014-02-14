package net.krazyweb.stardb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

import net.krazyweb.stardb.exceptions.StarDBException;

public class StarDBUtils {
	
	/**
	 * Reads some kind of size data from the file. The original implementation of this method in Python did not explain the acronym.
	 * @param byteChannel - The SeekableByteChannel to read from.
	 * @return A value of some kind, seemingly used to specify object data lengths.
	 * @throws StarDBException
	 */
	public static int readVLQU(final SeekableByteChannel byteChannel) throws StarDBException {
		
		int value = 0;
		
		while (true) {
			
			ByteBuffer buff = readToBuffer(byteChannel, 1);
			
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
	 * @return A ByteBuffer containing the requested data.
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
		buffer.rewind();
		
		return buffer;
		
	}
	
	/**
	 * Unpacks a Starbound database list into a list of Strings.
	 * @param data - The data to parse as a byte array.
	 * @return A list of Strings converted from Starbound's stored list format.
	 * @throws StarDBException An error occurred while unpacking the data.
	 */
	public static List<String> unpackStringList(final byte[] data) throws StarDBException {
		
		List<String> output = new ArrayList<>();
		
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.rewind();
		
		SeekableInMemoryByteChannel stream = new SeekableInMemoryByteChannel();
		
		try {
			stream.write(buffer);
			stream.position(0);
		} catch (IOException e) {
			try {
				stream.close();
			} catch (IOException e1) {
				throw new StarDBException("Error: " + e1.getMessage(), e1);
			}
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		int count = StarDBUtils.readVLQU(stream);
		
		for (int i = 0; i < count; i++) {
			int strLen = StarDBUtils.readVLQU(stream);
			ByteBuffer buff = readToBuffer(stream, strLen);
			output.add(new String(buff.array()));
		}
		
		return output;
		
	}

}
