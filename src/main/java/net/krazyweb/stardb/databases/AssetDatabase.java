package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class AssetDatabase extends SimpleSha256Database {
	
	private Set<String> fileList;
	
	private AssetDatabase(final BlockFile blockFile) {
		super(blockFile, "Assets1");
		fileList = null;
	}
	
	/**
	 * 
	 * @param databaseFile
	 * @return
	 * @throws IOException 
	 * @throws StarDBException 
	 */
	public static AssetDatabase open(final Path databaseFile) throws IOException, StarDBException {
		BlockFile bf = new BlockFile(databaseFile);
		AssetDatabase db = new AssetDatabase(bf);
		db.open();
		return db;
	}
	
	/**
	 * 
	 * @param databaseFile
	 * @return
	 * @throws IOException
	 * @throws StarDBException 
	 */
	public static AssetDatabase open(final String databaseFile) throws IOException, StarDBException {
		return open(Paths.get(databaseFile));
	}
	
	/**
	 * 
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws StarDBException
	 * @throws IOException
	 */
	public byte[] getDigest() throws NoSuchAlgorithmException, StarDBException, IOException {
		return getItem("_digest".getBytes());
	}
	
	/**
	 * 
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws StarDBException
	 * @throws IOException
	 */
	public Set<String> getFileList() throws NoSuchAlgorithmException, StarDBException, IOException {
		
		if (fileList != null) {
			return fileList;
		}
		
		fileList = unpackStringList(getItem("_index".getBytes()));
		
		return fileList;
		
	}
	
	/**
	 * 
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws StarDBException
	 * @throws IOException
	 */
	public Set<String> getBrokenFiles() throws NoSuchAlgorithmException, StarDBException, IOException {
		Set<String> output = new HashSet<>();
		for (String file : getFileList()) {
			if (file.length() == 55) {
				output.add(file);
			}
		}
		return output;
	}
	
	private Set<String> unpackStringList(final byte[] data) throws IOException, StarDBException {
		
		Set<String> output = new HashSet<>();
		
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.rewind();
		
		SeekableInMemoryByteChannel stream = new SeekableInMemoryByteChannel();
		stream.write(buffer);
		stream.position(0);
		
		int count = StarDBUtils.readVLQU(stream);
		
		for (int i = 0; i < count; i++) {
			int strLen = StarDBUtils.readVLQU(stream);
			ByteBuffer buff = ByteBuffer.allocate(strLen);
			buff.order(ByteOrder.BIG_ENDIAN);
			stream.read(buff);
			buff.rewind();
			output.add(new String(buff.array()));
		}
		
		return output;
		
	}
	
}
