package net.krazyweb.stardb.databases;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class SimpleSha256Database extends SimpleDatabase {
	
	/**
	 * Creates a database that uses SHA256 hashes as keys.
	 * @param blockFile - The file containing the database.
	 * @param contentID - The identifier for the content of the database.
	 */
	protected SimpleSha256Database(final BlockFile blockFile, String contentID) {
		super(blockFile, contentID, 32);
	}
	
	@Override
	protected byte[] find(final byte[] key) throws StarDBException {
		
		MessageDigest md = null;
		
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new StarDBException("Error: " + e.getMessage(), e);
		}
		
		byte[] digest = md.digest(key);
		
		return super.find(digest);
		
	}

	/**
	 * Finds the data for a given hash value.
	 * @param hash - The hash key to search for.
	 * @return The data for the given hash value or null if no data was found.
	 * @throws StarDBException An error occurred while retrieving the data for the given hash value.
	 */
	protected byte[] findByHash(final byte[] hash) throws StarDBException {
		return super.find(hash);
	}
	
}
