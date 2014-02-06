package net.krazyweb.stardb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import net.krazyweb.stardb.exceptions.StarDBException;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class BlockFile extends BlockStorage {

	private final String headerMagic = "SBBF02";
	private final int prefixHeaderSize = 32;
	
	/**
	 * TODO
	 * @param filePath
	 * @throws IOException 
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
	 * TODO
	 * @param filePath
	 * @throws IOException 
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
	 * 
	 * @return
	 */
	public int getUserHeaderSize() {
		return headerSize - prefixHeaderSize;
	}
	
	@Override
	public SeekableInMemoryByteChannel readBlock(int blockIndex, int blockOffset, int size) throws StarDBException, IOException {
		
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
		
		dataFile.position(blockStart + (blockIndex * blockSize) + blockOffset);
		dataFile.read(buffer);
		buffer.rewind();
		
		SeekableInMemoryByteChannel output = new SeekableInMemoryByteChannel();
		output.write(buffer);
		output.position(0);
		
		return output;
		
	}
	
	@Override
	public SeekableInMemoryByteChannel readBlock(int blockIndex) throws StarDBException, IOException {
		return readBlock(blockIndex, 0, 0);
	}
	
	@Override
	public SeekableInMemoryByteChannel readUserData(int dataOffset, int size) throws StarDBException, IOException {
		
		checkIfOpen(true);
		
		if (dataOffset + size > getUserHeaderSize()) {
			throw new StarDBException("readUserData() called outside of bounds of user header");
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(size);
		
		dataFile.position(prefixHeaderSize + dataOffset);
		dataFile.read(buffer);
		buffer.rewind();

		SeekableInMemoryByteChannel output = new SeekableInMemoryByteChannel();
		output.write(buffer);
		output.position(0);
		
		return output;
		
	}

	@Override
	public void open() throws StarDBException, IOException {
		
		checkIfOpen(false);
		
		if (dataFile == null) {
			throw new StarDBException("open() called with no file set.");
		}
		
		long previousPosition = dataFile.position();
		
		ByteBuffer buffer = ByteBuffer.allocate(6);

		dataFile.position(0);
		dataFile.read(buffer);
		buffer.rewind();
		
		String magic = new String(buffer.array());
		
		if (!magic.equals(headerMagic)) {
			throw new StarDBException("File is not a valid BlockFile");
		}
		
		buffer = ByteBuffer.allocate(13);
		buffer.order(ByteOrder.BIG_ENDIAN);
		
		dataFile.read(buffer);
		buffer.rewind();
		
		headerSize = buffer.getInt();
		blockSize = buffer.getInt();
		
		byte noFreeIndexBlock = buffer.get();
		if (noFreeIndexBlock == 0) {
			headFreeIndexBlock = buffer.getInt();
		}
		
		//TODO: I think if noFreeIndexBlock == True then we need recovery
		
		setExtents(headerSize, dataFile.size());
		
		open = true;
		
		dataFile.position(previousPosition);
		
	}
	
}
