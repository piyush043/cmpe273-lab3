package edu.sjsu.cmpe.cache.client.rendezvousHash;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;

public class RendezvousHash<K, S> {
	private final HashFunction hashFunction;
	private Funnel<K> keyFunnel;
	private Funnel<S> serverFunnel;
	private Set<S> serverSet;
	private volatile S[] sortedServerArray;
	
	public RendezvousHash(HashFunction hf, Funnel<K> kf, Funnel<S> sf, List<S> servers) {
		this.hashFunction = hf;
		this.keyFunnel = kf;
		this.serverFunnel = sf;
		this.serverSet = Sets.newHashSet();
		for(S server: (S[]) servers.toArray()){
			add(server);
		}
	}
	
	public synchronized void add(S server) {
		serverSet.add(server);
		S[] tmpArray = (S[]) serverSet.toArray();
		Arrays.sort(tmpArray);
		sortedServerArray = tmpArray;
	}
	
	public synchronized void remove(S server) {
		serverSet.remove(server);
		S[] tmpArray = (S[]) serverSet.toArray();
		Arrays.sort(tmpArray);
		sortedServerArray = tmpArray;
	}
	
	public S get(K key) {
		Long max = Long.MIN_VALUE;
		S result = null;
		for(S server: sortedServerArray) {
			Long serverHash = hashFunction.newHasher().putObject(key, keyFunnel).putObject(server, serverFunnel).hash().asLong();
			if(serverHash> max) {
				result = server;
				max = serverHash;
			}
		}
		return result;
	}
}
