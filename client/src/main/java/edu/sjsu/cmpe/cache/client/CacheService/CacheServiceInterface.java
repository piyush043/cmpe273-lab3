package edu.sjsu.cmpe.cache.client.cacheService;

/**
 * Cache Service Interface
 * 
 */
public interface CacheServiceInterface {
    public String get(long key);

    public void put(long key, String value);
}
