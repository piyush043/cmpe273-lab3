package edu.sjsu.cmpe.cache.repository;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import edu.sjsu.cmpe.cache.domain.Entry;

public class ChronicleMapCache implements CacheInterface{
    private static String tmpDir = System.getProperty("java.io.tmpdir");
    private static String pathname = tmpDir + "/";
    private File file;
	private ConcurrentMap<Long, Entry> map;
	
	public ChronicleMapCache(String fileName) {
		ChronicleMapBuilder<Long, Entry> builder = ChronicleMapBuilder.of(Long.class, Entry.class)
				.entries(100);
		pathname = pathname + fileName + ".txt";
		file = new File(pathname);
		
		try{
			map = builder.createPersistedTo(file);
			
		}
		catch(IOException e){
			e.printStackTrace();
		}
	
	}
	
	@Override
	public Entry save(Entry newEntry) {
		checkNotNull(newEntry, "New Entry should not be null");
		map.put(newEntry.getKey(), newEntry);
		return newEntry;
	}

	@Override
	public Entry get(Long key) {
		checkArgument(key > 0,
                "Key was %s but expected greater than zero value", key);
		
		return map.get(key);
	}

	@Override
	public List<Entry> getAll() {
		return new ArrayList<Entry>(map.values());
	}

}
