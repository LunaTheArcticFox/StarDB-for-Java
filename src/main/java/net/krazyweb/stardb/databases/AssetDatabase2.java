package net.krazyweb.stardb.databases;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.krazyweb.stardb.StarDBUtils;
import net.krazyweb.stardb.exceptions.StarDBException;
import net.krazyweb.stardb.storage.BlockFile;

public class AssetDatabase2 extends AssetDatabase {

	protected AssetDatabase2(final BlockFile blockFile) {
		super(blockFile, "Assets2");
	}
	
	@Override
	public List<String> getFileList() throws StarDBException {
		
		if (fileList != null) {
			return fileList;
		}
		
		fileList = StarDBUtils.unpackStringList(getItem("_index".getBytes()), 2);
		
		Set<String> toRemove = new HashSet<>();
		
		for (int i = 0; i < fileList.size(); i++) {
			if (i % 2 == 1) {
				//System.out.println(fileList.get(i));
				toRemove.add(fileList.get(i));
			}
		}
		
		fileList.removeAll(toRemove);
		
		return fileList;
		
	}
	
}
