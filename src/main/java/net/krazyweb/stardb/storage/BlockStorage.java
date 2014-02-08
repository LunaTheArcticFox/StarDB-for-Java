package net.krazyweb.stardb.storage;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.exceptions.StarDBException;

public abstract class BlockStorage {
	
	protected SeekableByteChannel dataFile;
	protected boolean open;
	protected int headerSize;
	protected int headFreeIndexBlock;
	protected int blockSize;
	protected long blockStart;
	protected long blockEnd;
	protected long blockCount;
	
	/**
	 * Creates a new BlockStorage instance from which to access a Starbound database file's contents.
	 */
	protected BlockStorage() {
		open = false;
	}
	
	/**
	 * Verifies that the file is in the correct state.
	 * @param mustBeOpened - The state the file must be in when checking.
	 * @throws StarDBException The file is not in the correct state.
	 */
	protected void checkIfOpen(final boolean mustBeOpened) throws StarDBException {
		if (!mustBeOpened && open) {
			throw new StarDBException("BlockStorage not opened, must be opened!");
		}
		if (mustBeOpened && !open) {
			throw new StarDBException("BlockStorage is opened, must not be opened!");
		}
	}
	
	/**
	 * Returns the block size.
	 * @return - The size of each block of data in bytes.
	 */
	public int getBlockSize() {
		return blockSize;
	}
	
	/**
	 * Reads a block of data into a SeekableByteChannel for further manipulation.
	 * @param blockIndex - The index of the block in the database file.
	 * @param blockOffset - The offset in bytes from which to start reading in data.
	 * @param size - The amount of data to read in bytes.
	 * @return - A SeekableByteChannel containing the data of the specified block.
	 * @throws StarDBException The block index is either out of range, no data would be read, or an error occurred while reading the data.
	 */
	public abstract SeekableByteChannel readBlock(final int blockIndex, final int blockOffset, final int size) throws StarDBException;
	
	/**
	 * Reads a block of data into a SeekableByteChannel for further manipulation.
	 * @param blockIndex - The index of the block in the database file.
	 * @return - A SeekableByteChannel containing the data of the specified block.
	 * @throws StarDBException The block index is either out of range or no data would be read.
	 * @throws IOException An IO problem occurred while trying to access the data channel.
	 */
	public abstract SeekableByteChannel readBlock(final int blockIndex) throws StarDBException;
	
	/**
	 * Reads the specified portion of the user data header in the file.
	 * @param dataOffset - The offset in bytes from which to begin reading data.
	 * @param size - The amount of data to read in bytes.
	 * @return A SeekableByteChannel containing the header information for the userdata.
	 * @throws StarDBException The method was called outside of the bounds of the user header or an error occurred while reading the data.
	 */
	public abstract SeekableByteChannel readUserData(final int dataOffset, final int size) throws StarDBException;
	
	/**
	 * Opens the database file and retrieves necessary data from it.
	 * @throws StarDBException No file is set or the file is not a valid BlockFile or an IO problem occurred while trying to access the database file.
	 */
	public abstract void open() throws StarDBException;
	
}
