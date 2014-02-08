package net.krazyweb.stardb.databases;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.btree.BTreeDatabase;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class SimpleDatabase extends BTreeDatabase {
	
	private String contentIdentifier;
	private int keySize;
	
	/**
	 * Creates a new SimpleDatabase, which implements some basic database access methods.
	 * @param blockFile - The file containing the database.
	 * @param contentID - The identifier for the content of the database.
	 * @param keySize - The size of each key in the database.
	 */
	protected SimpleDatabase(final BlockFile blockFile, final String contentID, final int keySize) {
		super(blockFile);
		contentIdentifier = contentID;
		this.keySize = keySize;
	}

	@Override
	protected int getKeySize() {
		return keySize;
	}

	@Override
	protected String getContentIdentifier() {
		return contentIdentifier;
	}

	@Override
	protected byte[] readKey(final SeekableByteChannel byteChannel) throws StarDBException {
		ByteBuffer buffer = StarDBUtils.readToBuffer(byteChannel, keySize);
		return buffer.array();
	}

	@Override
	protected byte[] readData(final SeekableByteChannel byteChannel) throws StarDBException {
		
		int size = StarDBUtils.readVLQU(byteChannel);

		ByteBuffer buffer = StarDBUtils.readToBuffer(byteChannel, size);
		return buffer.array();
		
	}
	
}
