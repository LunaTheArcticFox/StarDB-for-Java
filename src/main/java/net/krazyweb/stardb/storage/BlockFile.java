package net.krazyweb.stardb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.exceptions.StarDBException;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class BlockFile extends BlockStorage {

	private final String headerMagic = "SBBF02";
	private final int prefixHeaderSize = 32;
	
	/**
	 * Creates a new BlockFile instance from which to read a Starbound database file.
	 * @param filePath - The path to the database file on disk.
	 * @throws IOException - Could not open the database file.
	 */
	public BlockFile(final Path filePath) throws IOException {
		
		dataFile = Files.newByteChannel(filePath, StandardOpenOption.READ);
		
		open = false;
		headerSize = 256;
		headFreeIndexBlock = 0; //TODO (self.headFreeIndexBlock = None)
		blockSize = 1024;
		blockStart = 0;
		blockEnd = 0;
		blockCount = 0;
		
	}
	
	/**
	 * Creates a new BlockFile instance from which to read a Starbound database file.
	 * @param filePath - The path to the database file on disk.
	 * @throws IOException - Could not open the database file.
	 */
	public BlockFile(final String filePath) throws IOException {
		this(Paths.get(filePath));
	}
	
	private void setExtents(final long start, final long end) {
		
		blockStart = start;
		blockEnd = end;
		
		if (blockEnd < blockStart) {
			blockEnd = blockStart;
		}
		
		blockCount = blockEnd - blockStart; //(blockSize)
		
	}
	
	/**
	 * Returns the total size of the user header.
	 * @return - The size of the user header in bytes.
	 */
	public int getUserHeaderSize() {
		return headerSize - prefixHeaderSize;
	}
	
	@Override
	public SeekableByteChannel readBlock(int blockIndex, int blockOffset, int size) throws StarDBException {
		
		checkIfOpen(true);
		
		if (blockIndex > blockCount) {
			throw new StarDBException("Block index: " + blockIndex + " out of block range.");
		}
		
		if (size == 0) {
			size = blockSize - blockOffset;
		}
		
		blockOffset = Math.min(blockSize, blockOffset);
		size = Math.min(blockSize - blockOffset, size);
		
		if (size <= 0) {
			throw new StarDBException("No data would be read (" + blockOffset + ", " + size + ")");
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(size);
		
		try {
			dataFile.position(blockStart + (blockIndex * blockSize) + blockOffset);
			buffer = StarDBUtils.readToBuffer(dataFile, size);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		SeekableInMemoryByteChannel output = new SeekableInMemoryByteChannel();
		
		try {
			output.write(buffer);
			output.position(0);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		return output;
		
	}
	
	@Override
	public SeekableByteChannel readBlock(int blockIndex) throws StarDBException {
		return readBlock(blockIndex, 0, 0);
	}
	
	@Override
	public SeekableByteChannel readUserData(int dataOffset, int size) throws StarDBException {
		
		checkIfOpen(true);
		
		if (dataOffset + size > getUserHeaderSize()) {
			throw new StarDBException("readUserData() called outside of bounds of user header");
		}
		
		ByteBuffer buffer;
		
		try {
			dataFile.position(prefixHeaderSize + dataOffset);
			buffer = StarDBUtils.readToBuffer(dataFile, size);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}

		SeekableByteChannel output = new SeekableInMemoryByteChannel();
		
		try {
			output.write(buffer);
			output.position(0);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		return output;
		
	}

	@Override
	public void open() throws StarDBException {
		
		checkIfOpen(false);
		
		if (dataFile == null) {
			throw new StarDBException("open() called with no file set.");
		}
		
		long previousPosition = -1;
		
		try {
			previousPosition = dataFile.position();
			dataFile.position(0);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		ByteBuffer buffer = null;
		
		buffer = StarDBUtils.readToBuffer(dataFile, 6);
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(headerMagic)) {
			throw new StarDBException("File is not a valid BlockFile");
		}
		
		buffer = StarDBUtils.readToBuffer(dataFile, 13);
		
		headerSize = buffer.getInt();
		blockSize = buffer.getInt();
		
		byte noFreeIndexBlock = buffer.get();
		if (noFreeIndexBlock == 0) {
			headFreeIndexBlock = buffer.getInt();
		}
		
		try {
			setExtents(headerSize, dataFile.size());
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		open = true;
		
		try {
			dataFile.position(previousPosition);
		} catch (IOException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
	}
	
}
