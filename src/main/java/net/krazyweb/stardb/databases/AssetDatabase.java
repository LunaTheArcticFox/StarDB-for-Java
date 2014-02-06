package net.krazyweb.stardb.databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel;

public class AssetDatabase extends SimpleSha256Database {
	
	private Set<String> fileList;
	
	public AssetDatabase(final BlockFile blockFile) {
		super(blockFile, "Assets1");
		fileList = null;
	}
	
	public Object getDigest() throws NoSuchAlgorithmException, StarDBException, IOException {
		return getItem("_digest".getBytes());
	}
	
	public Set<String> getFileList() throws NoSuchAlgorithmException, StarDBException, IOException {
		
		if (fileList != null) {
			return fileList;
		}
		
		fileList = unpackStringList(getItem("_index".getBytes()));
		
		return fileList;
		
	}
	
	public Set<String> unpackStringList(final byte[] data) throws IOException, StarDBException {
		
		Set<String> output = new HashSet<>();
		
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.rewind();
		
		SeekableInMemoryByteChannel stream = new SeekableInMemoryByteChannel();
		stream.write(buffer);
		stream.position(0);
		
		int count = readVLQU(stream);
		
		for (int i = 0; i < count; i++) {
			int strLen = readVLQU(stream);
			ByteBuffer buff = ByteBuffer.allocate(strLen);
			buff.order(ByteOrder.BIG_ENDIAN);
			stream.read(buff);
			buff.rewind();
			output.add(new String(buff.array()));
		}
		
		return output;
		
	}
	
	/*
	 * def getBrokenFiles(self):
        brokenFiles = []
        for name in self.getFileList():
            if len(name) == 55:
                brokenFiles.append(name)
        return brokenFiles
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
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, StarDBException {
		
		BlockFile bf = new BlockFile(Paths.get("D:\\Games\\Steam\\steamapps\\common\\Starbound\\assets\\packed.pak"));
		AssetDatabase db = new AssetDatabase(bf);
		db.open();
		
		System.out.println(
			new String(
				db.getItem("/player.config".getBytes())
			)
		);
		
		System.out.println(db.getBrokenFiles());
		
	}
	
}
