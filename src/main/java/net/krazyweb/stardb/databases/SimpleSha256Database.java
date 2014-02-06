package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class SimpleSha256Database extends SimpleDatabase {
	
	public SimpleSha256Database(final BlockFile blockFile, String contentID) {
		super(blockFile, contentID, 32);
	}
	
	public Object find(final byte[] key) throws StarDBException, IOException, NoSuchAlgorithmException {
		
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		byte[] digest = md.digest(key);
		
		return super.find(digest);
		
	}
	
	public Object findByHash(final byte[] hash) throws NoSuchAlgorithmException, StarDBException, IOException {
		return super.find(hash);
	}
	
}
