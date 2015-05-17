package edu.sjsu.cmpe.cache.client.consistentHash;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;

public class ConsistentHash<K, S> {
	private final HashFunction hashFunction;
	private final SortedMap<Long, S> circle = new TreeMap<Long, S>();
	private Funnel<K> keyFunnel;
	private Funnel<S> serverFunnel;
	
	public ConsistentHash(HashFunction hf, Funnel<K> kf, Funnel<S> sf, List<S> servers) {
		this.hashFunction = hf;
		this.keyFunnel = kf;
		this.serverFunnel = sf;
		for(S server: servers) {
			add(server);
		}
	}

	public void add(S server) {
			circle.put(hashFunction.newHasher().putObject(server, serverFunnel).hash().asLong(), server);
	}

	public void remove(S server) {
			circle.remove(hashFunction.newHasher().putObject(server, serverFunnel).hash().asLong());
	}

	public S get(K key) {
		if (circle.isEmpty()) {
			return null;
		}
		Long hash = hashFunction.newHasher().putObject(key, keyFunnel).hash().asLong();
		if (!circle.containsKey(hash)) {
			SortedMap<Long, S> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
}
