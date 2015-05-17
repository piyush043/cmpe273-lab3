package edu.sjsu.cmpe.cache.client.consistentHash;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;

import edu.sjsu.cmpe.cache.client.cacheService.CacheServiceInterface;
import edu.sjsu.cmpe.cache.client.cacheService.DistributedCacheService;
public class ConsistentHashClient {
	
	private static List<String> servers = new ArrayList<String>();
	private static final Funnel<CharSequence> serverFunnel = Funnels.stringFunnel(Charset.defaultCharset());
	private static final Funnel<Integer> keyFunnel = Funnels.integerFunnel();
	private static char arr[] = {'a','b','c','d','e','f','g','h','i','j'};
	
	public static void main(String[] args) {
		System.out.println("Consistent Hash:");
		servers.add("http://localhost:3000");
		servers.add("http://localhost:3001");
		servers.add("http://localhost:3002");
		for(int i=0; i<10; i++) {
			ConsistentHash<Integer, String> consistentHash = new ConsistentHash(Hashing.murmur3_128(), keyFunnel, serverFunnel, servers) ;
			String serverUrl = consistentHash.get(new Integer(i));
			CacheServiceInterface cacheServer = new DistributedCacheService(serverUrl);
			cacheServer.put(i+1, String.valueOf(arr[i]));
			String res = cacheServer.get(i+1);
			System.out.println( "Server: "+ serverUrl + "\t"+ (i+1) + "=> " + res);
		}
	}
}
