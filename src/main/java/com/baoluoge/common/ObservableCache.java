package com.baoluoge.common;

import rx.Observable;

import java.util.Map;

/**
 * Created by PaulZhang on 2016/5/3.
 */
public interface ObservableCache {

    void add(String key, Object value, int expiration);

    Observable<Boolean> safeAdd(String key, Object value, int expiration);

    void set(String key, Object value, int expiration);

    Observable<Boolean> safeSet(String key, Object value, int expiration);

    void replace(String key, Object value, int expiration);

    Observable<Boolean> safeReplace(String key, Object value, int expiration);

    Observable<Object> get(String key);

    Observable<Map<String, Object>> get(String[] keys);

    Observable<Long> incr(String key, int by);

    Observable<Long> decr(String key, int by);

    void clear();

    void delete(String key);

    Observable<Boolean> safeDelete(String key);

    void stop();
}
