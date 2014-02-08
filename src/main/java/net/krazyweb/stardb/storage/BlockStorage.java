package net.krazyweb.stardb.storage;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.exceptions.StarDBException;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public abstract class BlockStorage {
	
	protected SeekableByteChannel dataFile;
	protected boolean open;
	protected int headerSize;
	protected int headFreeIndexBlock;
	protected int blockSize;
	protected long blockStart;
	protected long blockEnd;
	protected long blockCount;
	
	protected BlockStorage() {
		open = false;
	}
	
	protected void checkIfOpen(final boolean mustBeOpened) throws StarDBException {
		if (!mustBeOpened && open) {
			throw new StarDBException("BlockStorage not opened, must be opened!");
		}
		if (mustBeOpened && !open) {
			throw new StarDBException("BlockStorage is opened, must not be opened!");
		}
	}
	
	public int getBlockSize() {
		return blockSize;
	}
	
	public abstract SeekableInMemoryByteChannel readBlock(final int blockIndex, final int blockOffset, final int size) throws StarDBException, IOException;
	public abstract SeekableInMemoryByteChannel readBlock(final int blockIndex) throws StarDBException, IOException;
	public abstract SeekableInMemoryByteChannel readUserData(final int dataOffset, final int size) throws StarDBException, IOException;
	public abstract void open() throws StarDBException, IOException;
	
}
