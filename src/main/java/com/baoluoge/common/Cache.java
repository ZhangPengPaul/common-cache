package com.baoluoge.common;

import com.baoluoge.common.util.Time;
import rx.Observable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by PaulZhang on 2016/5/3.
 */
public class Cache {

    private static ObservableCache cacheImpl;

    public static Properties properties;

    /**
     * Initialize the cache system.
     */
    public static void init() {
        properties = new Properties();
        InputStream in = new BufferedInputStream(Cache.class.getResourceAsStream(CacheConfig.CLIENT_CONFIG_FILE));
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (Objects.equals(properties.getProperty("cache.type"), "memcached")) {
            try {
                cacheImpl = MemcachedImpl.getInstance(true);
            } catch (IOException e) {
                e.printStackTrace();
                // TODO fall back to local cache
            }
        } else {
            // TODO init local cache
        }
    }

    /**
     * add an object only if it doesn't exits
     *
     * @param key
     * @param value
     * @param expiration
     */
    public static void add(String key, Object value, String expiration) throws Exception {
        checkSerializable(value);
        cacheImpl.add(key, value, Time.parseDuration(expiration));
    }

    /**
     * add an object only if it doesn't exits, and return only if the element is effectively cached.
     *
     * @param key
     * @param value
     * @param expiration
     * @return
     * @throws Exception
     */
    public static boolean safaAdd(String key, Object value, String expiration) throws Exception {
        checkSerializable(value);
        Observable<Boolean> observable = cacheImpl.safeAdd(key, value, Time.parseDuration(expiration));
        return observable.toBlocking().single();
    }

    /**
     * add an element only if it doesn't exist and store it indefinitely.
     *
     * @param key
     * @param value
     * @throws Exception
     */
    public static void add(String key, Object value) throws Exception {
        checkSerializable(key);
        cacheImpl.add(key, value, Time.parseDuration(null));
    }

    /**
     * set an element
     *
     * @param key
     * @param value
     * @param expiration
     * @throws Exception
     */
    public static void set(String key, Object value, String expiration) throws Exception {
        checkSerializable(key);
        cacheImpl.set(key, value, Time.parseDuration(expiration));
    }

    /**
     * set an element and return only when the element is effectively cached.
     *
     * @param key
     * @param value
     * @param expiration
     * @return
     * @throws Exception
     */
    public static boolean safeSet(String key, Object value, String expiration) throws Exception {
        checkSerializable(key);
        Observable<Boolean> observable = cacheImpl.safeSet(key, value, Time.parseDuration(expiration));
        return observable.toBlocking().single();
    }

    /**
     * set an element and store it indefinitely.
     *
     * @param key
     * @param value
     * @throws Exception
     */
    public static void set(String key, Object value) throws Exception {
        checkSerializable(value);
        cacheImpl.set(key, value, Time.parseDuration(null));
    }

    /**
     * replace an element only if it already exists.
     *
     * @param key
     * @param value
     * @param expiration
     * @throws Exception
     */
    public static void replace(String key, Object value, String expiration) throws Exception {
        checkSerializable(value);
        cacheImpl.replace(key, value, Time.parseDuration(expiration));
    }

    /**
     * replace an element only if it already exists and return only when the element is effectively cached.
     *
     * @param key
     * @param value
     * @param expiration
     * @return
     * @throws Exception
     */
    public static boolean safeReplace(String key, Object value, String expiration) throws Exception {
        checkSerializable(value);
        Observable<Boolean> observable = cacheImpl.safeReplace(key, value, Time.parseDuration(expiration));
        return observable.toBlocking().single();
    }

    /**
     * replace an element only if it already exists and store it indefinitely.
     *
     * @param key
     * @param value
     * @throws Exception
     */
    public static void replace(String key, Object value) throws Exception {
        checkSerializable(value);
        cacheImpl.replace(key, value, Time.parseDuration(null));
    }

    /**
     * increment the element value (must be a Number).
     *
     * @param key
     * @param by
     * @return
     */
    public static long incr(String key, int by) {
        Observable<Long> observable = cacheImpl.incr(key, by);
        return observable.toBlocking().single();
    }

    /**
     * Increment the element value (must be a Number) by 1.
     *
     * @param key
     * @return
     */
    public static long incr(String key) {
        Observable<Long> observable = cacheImpl.incr(key, 1);
        return observable.toBlocking().single();
    }

    /**
     * Decrement the element value (must be a Number).
     *
     * @param key
     * @param by
     * @return
     */
    public static long decr(String key, int by) {
        Observable<Long> observable = cacheImpl.decr(key, by);
        return observable.toBlocking().single();
    }

    /**
     * Decrement the element value (must be a Number) by 1.
     *
     * @param key
     * @return
     */
    public static long decr(String key) {
        Observable<Long> observable = cacheImpl.decr(key, 1);
        return observable.toBlocking().single();
    }

    /**
     * Retrieve an object.
     *
     * @param key
     * @return
     */
    public static Object get(String key) {
        Observable<Object> observable = cacheImpl.get(key);
        return observable.toBlocking().single();
    }

    /**
     * Bulk retrieve.
     *
     * @param key
     * @return
     */
    public static Map<String, Object> get(String... key) {
        Observable<Map<String, Object>> observable = cacheImpl.get(key);
        return observable.toBlocking().single();
    }

    /**
     * Delete an element from the cache.
     *
     * @param key
     */
    public static void delete(String key) {
        cacheImpl.delete(key);
    }

    /**
     * Delete an element from the cache and return only when the element is effectively removed.
     *
     * @param key
     * @return
     */
    public static boolean safeDelete(String key) {
        Observable<Boolean> observable = cacheImpl.safeDelete(key);
        return observable.toBlocking().single();
    }

    /**
     * Clear all data from cache.
     */
    public static void clear() {
        cacheImpl.clear();
    }

    /**
     * Convenient clazz to get a value a class type;
     *
     * @param key
     * @param clazz
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(String key, Class<T> clazz) {
        Observable<Object> observable = cacheImpl.get(key);
        return (T) observable.toBlocking().single();
    }

    /**
     * Stop the cache system.
     */
    public static void stop() {
        cacheImpl.stop();
    }

    /**
     * check object is serializable.
     *
     * @param value
     * @throws Exception
     */
    private static void checkSerializable(Object value) throws Exception {
        if (value != null && !(value instanceof Serializable)) {
            throw new Exception("Cannot cache a non-serializable value of type " + value.getClass().getName());
        }
    }


}
