package com.baoluoge.common;

import com.baoluoge.common.exception.ConfigurationException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.transcoders.SerializingTranscoder;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by PaulZhang on 2016/5/3.
 */
public class MemcachedImpl implements ObservableCache {

    private static MemcachedImpl uniqueInstance;

    MemcachedClient client;

    SerializingTranscoder tc;

    public static MemcachedImpl getInstance() throws IOException {
        return getInstance(false);
    }

    public static MemcachedImpl getInstance(boolean forceClientInit) throws IOException {
        if (uniqueInstance == null) {
            uniqueInstance = new MemcachedImpl();
        } else if (forceClientInit) {
            // When you stop the client, it sets the interrupted state of this thread to true. If you try to reinit it with the same thread in this state,
            // Memcached client errors out. So a simple call to interrupted() will reset this flag
            Thread.interrupted();
            uniqueInstance.initClient();
        }
        return uniqueInstance;

    }

    private MemcachedImpl() throws IOException {
        tc = new SerializingTranscoder();
        initClient();
    }

    public void initClient() throws IOException {
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        List<InetSocketAddress> addresses;
        if (Cache.properties.containsKey("memcached.host")) {
            addresses = AddrUtil.getAddresses(Cache.properties.getProperty("memcached.host"));
        } else if (Cache.properties.containsKey("memcached.1.host")) {
            int n = 1;
            StringBuilder addressBuilder = new StringBuilder();
            while (Cache.properties.containsKey("memcached." + n + ".host")) {
                addressBuilder.append(Cache.properties.get("memcached." + n + ".host")).append(" ");
                n++;
            }
            addresses = AddrUtil.getAddresses(addressBuilder.toString());
        } else {
            throw new ConfigurationException("Bad configuration for memcached: missing host(s)");
        }

        if (Cache.properties.containsKey("memcached.user")) {
            String memcachedUser = Cache.properties.getProperty("memcached.user");
            String memcachedPassword = Cache.properties.getProperty("memcached.password");
            if (Objects.isNull(memcachedPassword)) {
                throw new ConfigurationException("Bad configuration for memcached: missing password");
            }

            // Use plain SASL to connect to memcached
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"},
                    new PlainCallbackHandler(memcachedUser, memcachedPassword));
            ConnectionFactory connectionFactory = new ConnectionFactoryBuilder()
                    .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                    .setAuthDescriptor(ad)
                    .build();

            client = new MemcachedClient(connectionFactory, addresses);
        } else {
            client = new MemcachedClient(addresses);
        }
    }


    public void add(String key, Object value, int expiration) {
        Async.start((Func0<Future<Boolean>>) () -> client.add(key, expiration, value, tc),
                Schedulers.io());
    }

    public Observable<Boolean> safeAdd(String key, Object value, int expiration) {
        return Observable.create((Subscriber<? super Boolean> s) -> {
            Future<Boolean> future = client.add(key, expiration, value, tc);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                future.cancel(false);
                s.onError(e);
            }
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public void set(String key, Object value, int expiration) {
        Async.start((Func0<Future<Boolean>>) () -> client.set(key, expiration, value, tc),
                Schedulers.io());
    }

    public Observable<Boolean> safeSet(String key, Object value, int expiration) {
        return Observable.create((Subscriber<? super Boolean> s) -> {
            Future<Boolean> future = client.set(key, expiration, value, tc);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                future.cancel(false);
                s.onError(e);
            }
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public void replace(String key, Object value, int expiration) {
        Async.start((Func0<Future<Boolean>>) () -> client.replace(key, expiration, value, tc), Schedulers.io());

    }

    public Observable<Boolean> safeReplace(String key, Object value, int expiration) {
        return Observable.create((Subscriber<? super Boolean> s) -> {
            Future<Boolean> future = client.replace(key, expiration, value, tc);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                future.cancel(false);
                s.onError(e);
            }
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public Observable<Object> get(String key) {
        return Observable.create((Subscriber<? super Object> s) -> {
            Future<Object> future = client.asyncGet(key, tc);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                future.cancel(false);
                s.onError(e);
            }
        }).subscribeOn(Schedulers.io());
    }

    public Observable<Map<String, Object>> get(String[] keys) {
        return Observable.create((Subscriber<? super Map<String, Object>> s) -> {
            Future<Map<String, Object>> future = client.asyncGetBulk(tc, keys);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                future.cancel(false);
                s.onError(e);
            }
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public Observable<Long> incr(String key, int by) {
        return Observable.create((Subscriber<? super Long> s) -> {
            Long result = client.incr(key, by, 0);
            s.onNext(result);
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public Observable<Long> decr(String key, int by) {
        return Observable.create((Subscriber<? super Long> s) -> {
            Long result = client.decr(key, by, 0);
            s.onNext(result);
            s.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    public void clear() {
        Async.start((Func0<Future<Boolean>>) () -> client.flush(), Schedulers.io());
    }

    public void delete(String key) {
        Async.start((Func0<Future<Boolean>>) () -> client.delete(key), Schedulers.io());
    }

    public Observable<Boolean> safeDelete(String key) {
        return Observable.create((Subscriber<? super Boolean> s) -> {
            Future<Boolean> future = client.delete(key);
            try {
                s.onNext(future.get(1, TimeUnit.SECONDS));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).subscribeOn(Schedulers.io());
    }

    public void stop() {
        Async.start(() -> {
            client.shutdown();
            return null;
        }, Schedulers.io());
    }
}
