package net.krazyweb.stardb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;

import net.krazyweb.stardb.databases.AssetDatabase;
import net.krazyweb.stardb.exceptions.StarDBException;

public class Test {
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, StarDBException {
		
		AssetDatabase db = AssetDatabase.open("D:/Games/Steam/steamapps/common/Starbound/assets/packed.pak");
		
		System.out.println(
			new String(
				db.getItem("/player.config".getBytes())
			)
		);
		
		ByteBuffer buffer = ByteBuffer.wrap(db.getItem("/tentacletex.png".getBytes()));
		
		FileChannel out = FileChannel.open(Paths.get("test.png"), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		out.write(buffer);
		out.force(false);
		out.close();
		
		System.out.println(db.getBrokenFiles());
		
		//AssetDatabase db = new AssetDatabase("path/string");
		//db.getItem("string");
		//db.listItems();
		//db.listBrokenFiles();
		
	}
	
}