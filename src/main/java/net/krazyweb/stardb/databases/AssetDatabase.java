package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class AssetDatabase extends SimpleSha256Database {
	
	private List<String> fileList;
	
	private AssetDatabase(final BlockFile blockFile) {
		super(blockFile, "Assets1");
		fileList = null;
	}
	
	/**
	 * Creates a new AssetDatabase for the database file specified, then opens it and readies it for reading.
	 * @param databaseFile - The path of the database file.
	 * @return An AssetDatabase file from which to read Starbound asset files.
	 * @throws IOException An error occurred while trying to read the database file.
	 * @throws StarDBException An error occurred while trying to read the database file.
	 */
	public static AssetDatabase open(final Path databaseFile) throws IOException, StarDBException {
		BlockFile bf = new BlockFile(databaseFile);
		AssetDatabase db = new AssetDatabase(bf);
		db.open();
		return db;
	}
	
	/**
	 * Creates a new AssetDatabase for the database file specified, then opens it and readies it for reading.
	 * @param databaseFile - The path of the database file.
	 * @return An AssetDatabase file from which to read Starbound asset files.
	 * @throws IOException An error occurred while trying to read the database file.
	 * @throws StarDBException An error occurred while trying to read the database file.
	 */
	public static AssetDatabase open(final String databaseFile) throws IOException, StarDBException {
		return open(Paths.get(databaseFile));
	}
	
	/**
	 * Retrieves a file from the database.
	 * @param file - The path of the file to retrieve.
	 * @return The contents of the file as a byte array.
	 * @throws StarDBException An error occurred while reading the database.
	 */
	public byte[] getAsset(final String file) throws StarDBException {
		return getItem(file.getBytes());
	}
	
	/**
	 * This seems to be the SHA256 of the fileName + fileContents of every file in the database. 
	 * @return The SHA256 value of the fileName + fileContents of every file in the database.
	 * @throws StarDBException An error occurred while reading the database.
	 */
	public byte[] getDigest() throws StarDBException {
		return getItem("_digest".getBytes());
	}
	
	/**
	 * Gets the set of files from the database's index.
	 * @return A set containing the file paths of every file in the database.
	 * @throws StarDBException An error occurred while reading the database.
	 */
	public List<String> getFileList() throws StarDBException {
		
		if (fileList != null) {
			return fileList;
		}
		
		fileList = StarDBUtils.unpackStringList(getItem("_index".getBytes()));
		
		return fileList;
		
	}
	
	/**
	 * A bug (feature?) in Starbound's SHA256 implementation results in an invalid hash for any file with a path length of 55 characters. The file cannot be retrieved from the database without a valid hash. This returns the list of all files affected by this bug (feature?). 
	 * @return A set containing all files that cannot be retrieved from the database for whatever reason. An empty set is returned if all files can be retrieved.
	 * @throws StarDBException An error occurred while reading the database.
	 */
	public List<String> getBrokenFileList() throws StarDBException {
		List<String> output = new ArrayList<>();
		for (String file : getFileList()) {
			if (file.length() == 55) {
				output.add(file);
			}
		}
		return output;
	}
	
}
