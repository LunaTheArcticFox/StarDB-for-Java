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
		return super.getItem("_digest".getBytes());
	}
	
	public Set<String> getFileList() throws NoSuchAlgorithmException, StarDBException, IOException {
		
		if (fileList != null) {
			return fileList;
		}
		
		String indexData = (String) super.getItem("_index".getBytes());
		
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
	/*
    # Since Starbound's SHA256 implementation is broken for 55 length strings,
    # this function will get you a list of all files which can't be found.
    def getBrokenFiles(self):
        brokenFiles = []
        for name in self.getFileList():
            if len(name) == 55:
                brokenFiles.append(name)
        return brokenFiles
	 */
	
	/*
	 * def unpackStringList(data):
    stream = BytesIO(data) # TODO: I think this makes a copy
    count = readVLQU(stream)
    strings = []
    for i in range(count):
        strLen = readVLQU(stream)
        strings.append(stream.read(strLen).decode('utf-8'))
    return strings
	 */
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, StarDBException {
		
		BlockFile bf = new BlockFile(Paths.get("D:\\Games\\Steam\\steamapps\\common\\Starbound\\assets\\packed.pak"));
		AssetDatabase db = new AssetDatabase(bf);
		db.open();
		System.out.println("__" + 
			new String(
				(byte[]) db.getItem("/weather/snow/snow.weather".getBytes())
			)
		);
		//System.out.println(db.getFileList());
		
	}
	
}
