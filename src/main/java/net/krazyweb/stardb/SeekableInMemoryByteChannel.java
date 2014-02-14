package net.krazyweb.stardb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

public class SeekableInMemoryByteChannel implements SeekableByteChannel {

	private int position;
	protected volatile boolean open;
	
	private byte[] data;
	
	public SeekableInMemoryByteChannel() {
		
		synchronized (this) {
			position = 0;
			data = new byte[0];
		}
		
		open = true;
		
	}
	
	@Override
	public void close() throws IOException {
		open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public long position() throws IOException {
		
		if (!open) {
			throw new ClosedChannelException();
		}
		
		synchronized (this) {
			return position;
		}
		
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		
		if (!open) {
			throw new ClosedChannelException();
		}
		
		if (newPosition < 0) {
			throw new IllegalArgumentException("Channel position cannot be negative.");
		}
		
		if (newPosition > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Channel position cannot be greater than Integer.MAX_VALUE.");
		}
		
		synchronized (this) {
			position = (int) newPosition;
		}
		
		return this;
		
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		
		if (!open) {
			throw new ClosedChannelException();
		}
		
		if (dst == null) {
			throw new IOException("A destination buffer must be supplied.");
		}
		
		int bytesToRead = dst.remaining();
		int bytesRead = 0;
		
		synchronized (this) {
			
			if (position > data.length) {
				return 0;
			}
			
			if (position + bytesToRead > data.length) {
				bytesToRead = data.length - position;
			}
			
			dst.put(Arrays.copyOfRange(data, position, position + bytesToRead));
			
			position += bytesToRead;
			
		}
		
		bytesRead = bytesToRead;
		
		return bytesRead;
		
	}

	@Override
	public long size() throws IOException {
		
		if (!open) {
			throw new ClosedChannelException();
		}
		
		synchronized (this) {
			return data.length;
		}
		
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {

		if (!open) {
			throw new ClosedChannelException();
		}
		
		if (size < 0) {
			throw new IllegalArgumentException("Size cannot be negative.");
		}
		
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Size cannot be greater than Integer.MAX_VALUE.");
		}
		
		synchronized (this) {
			
			if (data.length < size) {
				position = (int) size;
				return this;
			}

			if (position > size) {
				position = (int) size;
			}
			
			if (data.length > size) {
				
				data = Arrays.copyOfRange(data, 0, (int) size);
				
			}
			
		}
		
		return this;
		
	}

	@Override
	public int write(ByteBuffer src) throws IOException {

		if (!open) {
			throw new ClosedChannelException();
		}
		
		if (src == null) {
			throw new IOException("A source buffer must be supplied.");
		}
		
		int bytesToRead = src.remaining();
		
		synchronized (this) {
			
			byte[] toPosition = Arrays.copyOfRange(data, 0, position);
			byte[] afterPosition = Arrays.copyOfRange(data, position, data.length);
			byte[] toInsert = new byte[bytesToRead];
			src.get(toInsert);
			
			byte[] output = new byte[toPosition.length + afterPosition.length + toInsert.length];
			
			System.arraycopy(toPosition, 0, output, 0, toPosition.length);
			System.arraycopy(toInsert, 0, output, toPosition.length, toInsert.length);
			System.arraycopy(afterPosition, 0, output, toPosition.length + toInsert.length, afterPosition.length);
			
			data = output;
			
		}
		
		return bytesToRead;
		
	}
	
	
	
}