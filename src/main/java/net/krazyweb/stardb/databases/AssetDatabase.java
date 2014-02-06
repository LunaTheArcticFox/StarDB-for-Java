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
		
		String indexData = new String((byte[]) getItem("_index".getBytes()));
		
		fileList = unpackStringList(indexData);
		
		return fileList;
		
	}
	
	public Set<String> unpackStringList(final String data) throws IOException, StarDBException {
		
		Set<String> output = new HashSet<>();
		
		ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
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
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, StarDBException {
		
		BlockFile bf = new BlockFile(Paths.get("D:\\Games\\Steam\\steamapps\\common\\Starbound\\assets\\packed.pak"));
		AssetDatabase db = new AssetDatabase(bf);
		db.open();
		
		System.out.println(
			new String(
				(byte[]) db.getItem("/player.config".getBytes())
			)
		);
		
		System.out.println(db.getFileList());
		
	}
	
}
