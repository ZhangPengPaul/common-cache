package com.baoluoge.common;

import rx.Observable;

import java.util.Map;

/**
 * Created by PaulZhang on 2016/5/3.
 */
public interface ObservableCache {

    public void add(String key, Object value, int expiration);

    public Observable<Boolean> safeAdd(String key, Object value, int expiration);

    public void set(String key, Object value, int expiration);

    public Observable<Boolean> safeSet(String key, Object value, int expiration);

    public void replace(String key, Object value, int expiration);

    public Observable<Boolean> safeReplace(String key, Object value, int expiration);

    public Observable<Object> get(String key);

    public Observable<Map<String, Object>> get(String[] keys);

    public Observable<Long> incr(String key, int by);

    public Observable<Long> decr(String key, int by);

    public void clear();

    public void delete(String key);

    public boolean safeDelete(String key);

    public void stop();
}
