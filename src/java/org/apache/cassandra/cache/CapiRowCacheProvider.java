package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.cache.SerializingCacheProvider.RowCacheSerializer;
import org.apache.cassandra.cache.capi.CapiChunkDriver;
import org.apache.cassandra.cache.capi.SimpleCapiSpaceManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.schema.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.ibm.research.capiblock.CapiBlockDevice;

public class CapiRowCacheProvider implements org.apache.cassandra.cache.CacheProvider<RowCacheKey, IRowCacheEntry> {
    private static final Logger logger = LoggerFactory.getLogger(CapiRowCacheProvider.class);

    public static final int DEFAULT_L2CACHE = 128 * 1024 * 1024;
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    public static final String PROP_CAPI_DEVICE_NAMES = "capi.devices";
    public static final long DEFAULT_SIZE_IN_GB_BYTES = 1L; // 1 gb

    public static AtomicLong touched = new AtomicLong();
    public static AtomicLong cacheHit = new AtomicLong();
    public static AtomicLong cacheMiss = new AtomicLong();
    public static AtomicLong cachePush = new AtomicLong();
    public static AtomicLong swapin = new AtomicLong();
    public static AtomicLong swapinMiss = new AtomicLong();
    public static AtomicLong swapinErr = new AtomicLong();
    public static AtomicLong remove = new AtomicLong();

    public interface HashFunction {
        int hashCode(byte[] bb);

        String toString(byte[] bb);
    }

    @Override
    public ICache<RowCacheKey, IRowCacheEntry> create() {
        return new CapiRowCache(DatabaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024);
    }

    public static class CapiRowCache implements ICache<RowCacheKey, IRowCacheEntry> {

        private final Cache<RowCacheKey, IRowCacheEntry> map;
        private final ISerializer<IRowCacheEntry> serializer = new RowCacheSerializer();
        final HashFunction hashFunc;
        final AtomicInteger size = new AtomicInteger();
        final SimpleCapiSpaceManager sm;
        final int[] filters;
        final ReentrantLock[] monitors = new ReentrantLock[Runtime.getRuntime().availableProcessors() * 64];

        CapiRowCache(long capacity) {
            this.map = Caffeine.newBuilder().weigher(new Weigher<RowCacheKey, IRowCacheEntry>(){
				@Override
				public int weigh(RowCacheKey key, IRowCacheEntry value) {
					 long serializedSize = serializer.serializedSize(value);
	                    if (serializedSize > Integer.MAX_VALUE)
	                        throw new IllegalArgumentException("Unable to allocate " + serializedSize + " bytes");
	                    return (int) serializedSize;
				}
//            }).maximumWeight(DEFAULT_L2CACHE).build();
            }).maximumWeight(capacity).build();

            String hashClass = System.getProperty("capi.hash");
            try {
                hashFunc = hashClass == null ? new HashFunction() {
                    public int hashCode(byte[] key) {
                        return Arrays.hashCode(key);
                    }

                    public String toString(byte[] key) {
                        return "unknown";
                    }
                } : (HashFunction) Class.forName(hashClass).newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            int blocksize = CapiBlockDevice.BLOCK_SIZE;
            int numOfAsync = Integer.parseInt(System.getProperty("capi.async", "64"));
            int numOfDriver = Integer.parseInt(System.getProperty("capi.driver", "32"));

            String deviceNamesStr = System.getProperty(PROP_CAPI_DEVICE_NAMES);
            System.out.println ( " The deviceName string is : " + deviceNamesStr );
            if (deviceNamesStr == null) {
                logger.error("no valid device name in " + PROP_CAPI_DEVICE_NAMES);
                throw new IllegalStateException("no valid device name in " + PROP_CAPI_DEVICE_NAMES);
            }

            this.sm = new SimpleCapiSpaceManager();
            // -Dcapi.devices=/dev/sg7:<OFFSET>:<GB_SIZE>:/dev/sg7:/dev/sg8:/dev/sg8,/dev/sdc:<OFFSET>:<GB_SIZE>
            String[] deviceInfos = deviceNamesStr.split(",");

            StringBuffer storageArea = new StringBuffer();
            boolean first = true;
            for (String deviceInfo : deviceInfos) {
                try {
                    // /dev/sdb:<OFFSET>:<GB_SIZE>
                    String[] deviceAttrs = deviceInfo.split(":");
                    String deviceName = deviceAttrs[0];
                    long offset = deviceAttrs.length > 1 ? Long.parseLong(deviceAttrs[1]) : 0;
                    long sizeInBytes = 1024L * 1024L * 1024L * (deviceAttrs.length > 2 ? Long.parseLong(deviceAttrs[2]) : DEFAULT_SIZE_IN_GB_BYTES);
                    String[] devices = new String[1 + Math.max(0, deviceAttrs.length - 3)];
                    devices[0] = deviceName;
                    for (int i = 0; i < devices.length - 1; ++i)
                        devices[i + 1] = deviceAttrs[i + 3];
                    CapiChunkDriver driver = new CapiChunkDriver(devices, numOfAsync);
                    sm.add(driver, offset, sizeInBytes / (long) CapiBlockDevice.BLOCK_SIZE);

                    if (first)
                        first = false;
                    else
                        storageArea.append(",");
                    storageArea.append(deviceName + ":" + (sizeInBytes / 1024.0 / 1024.0 / 1024.0) + "gb [" + Long.toHexString(offset) + "-" + Long.toHexString(offset + (sizeInBytes / (long) CapiBlockDevice.BLOCK_SIZE) * (long) CapiBlockDevice.BLOCK_SIZE) + "]");
                    logger.info("capicache: device=" + deviceName + ", start=" + offset + ", size=" + (sizeInBytes / 1024.0 / 1024.0 / 1024.0) + "gb");
                } catch (IOException ex) {
                    logger.error("errors to create chunks for " + deviceNamesStr, ex);
                    throw new IllegalStateException(ex);
                }
            }

            try {
                sm.initialize(false);
            } catch (IOException ex) {
                logger.error("errors to initialize storage manager " + deviceNamesStr, ex);
                throw new IllegalStateException(ex);
            }

            long capiSize = sm.getLimitInBlocks();
            long maskSize = capiSize / 32 + (capiSize % 32 == 0 ? 0 : 1);
            filters = new int[(int) maskSize];

            for (int i = 0; i < monitors.length; ++i)
                monitors[i] = new ReentrantLock();

            logger.info("capi-rowcache: blocksize=" + (blocksize / 1024) + "kb, asynch=" + numOfAsync + ", driver=" + numOfDriver + ",  hash=" + hashFunc.getClass().getName() + ", area=" + storageArea + ", entry=" + capiSize);
        }

        public int hashCode(RowCacheKey key) {
            int result = key.tableId.hashCode();
            result = 31 * result + (key != null ? hashFunc.hashCode(key.key) : 0);
            return result;
        }

        public boolean equals(RowCacheKey key, ByteBuffer keyBB) {
        	TableId tableId = TableId.fromUUID(new UUID(keyBB.getLong(), keyBB.getLong()));
            if (!tableId.equals(key.tableId))
                return false;
            if (!readString(keyBB).equals(key.indexName))
                return false;
            if (keyBB.remaining() != key.key.length)
                return false;

            for (int i = 0; i < key.key.length; ++i)
                if (key.key[i] != keyBB.get())
                    return false;
            return true;
        }

        public void serializeKey(RowCacheKey key, ByteBuffer bb) {
            try {
                // logger.info("serializeKey=" + bb + ": " + key);
                bb.putLong(key.tableId.asUUID().getMostSignificantBits());
                bb.putLong(key.tableId.asUUID().getLeastSignificantBits());
                writeString(bb, key.indexName);
                bb.put(key.key);
            } catch (BufferOverflowException ex) {
                logger.error("ideal: " + (key.tableId.serializedSize() + getByteSizeForString(key.indexName) + key.key.length) + ", actual=" + bb.capacity());
                throw ex;
            }
        }

        public void serializeValue(IRowCacheEntry value, ByteBuffer bb) {
            try {
                serializer.serialize(value, new DataOutputBufferFixed(bb));
            } catch (IOException e) {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }

        public void writeString(ByteBuffer bb, String str) {
            byte[] bytes = str.getBytes();
            bb.putInt(bytes.length);
            bb.put(bytes);
        }

        public String readString(ByteBuffer bb) {
            int length = bb.getInt();
            byte[] bytes = new byte[length];
            bb.get(bytes);
            return new String(bytes);
        }

        public int getByteSizeForString(String str) {
            return str.getBytes().length + 4;
        }

        public RowCacheKey deserializeKey(ByteBuffer bb) {
            TableId tableId = TableId.fromUUID(new UUID(bb.getLong(), bb.getLong()));
            String indexName = readString(bb);
           
            ByteBuffer keyBody = ByteBuffer.allocateDirect(bb.remaining());
            keyBody.put(bb);
            return new RowCacheKey(tableId, indexName, keyBody);
        }

        public IRowCacheEntry deserializeValue(ByteBuffer bb) {
            try {
                return serializer.deserialize(new DataInputBuffer(bb, false));
            } catch (IOException e) {
                logger.debug("Cannot fetch in memory data, we will fallback to read from disk ", e);
                throw new IllegalStateException(e);
            }
        }

        public int keySize(RowCacheKey k) {
            return k.tableId.serializedSize() + getByteSizeForString(k.indexName) + k.key.length;
        }

        public int valueSize(IRowCacheEntry v) {
            int size = (int) serializer.serializedSize(v);

            if (size == Integer.MAX_VALUE)
                throw new IllegalStateException();

            return size + 1;
        }

        void lock(int hash) {
            hash = Math.abs(hash);
            ReentrantLock monitor = monitors[hash % monitors.length];
            monitor.lock();
        }

        void unlock(int hash) {
            hash = Math.abs(hash);
            ReentrantLock monitor = monitors[hash % monitors.length];
            monitor.unlock();
        }

        boolean exist(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash / 32 % filters.length;
                int maxPos = hash % 32;
                int mask = filters[maskIdx];
                int maskFilter = 1 << maxPos;
                return (mask & maskFilter) != 0;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        void filled(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash / 32 % filters.length;
                int maxPos = hash % 32;
                int maskFilter = 1 << maxPos;
                filters[maskIdx] |= maskFilter;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        void invalidate(int hash, boolean withLock) {
            hash = Math.abs(hash);

            ReentrantLock monitor = monitors[hash % monitors.length];
            if (withLock)
                monitor.lock();
            try {
                int maskIdx = hash / 32 % filters.length;
                int maxPos = hash % 32;
                int maskFilter = 1 << maxPos;
                filters[maskIdx] &= ~maskFilter;
            } finally {
                if (withLock)
                    monitor.unlock();
            }
        }

        long getLBA(int hash) {
            return Math.abs(hash) % sm.getLimitInBlocks();
        }

        @Override
        public long capacity() {
            return sm.getLimitInBytes();
        }

        @Override
        public void setCapacity(long capacity) {
            logger.warn("setCapacity is not supportted.");
        }

        @Override
        public int size() {
            return size.get();
        }

        @Override
        public long weightedSize() {
            return map.estimatedSize();
        }

        @Override
        public void put(RowCacheKey key, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                putToCapi(key, hash, value, false);
                map.put(key, value);
                filled(hash, false);

                if (cachePush.incrementAndGet() % logUnit == 0)
                    log();

            } finally {
                unlock(hash);
            }
        }

        private void putToCapi(RowCacheKey key, int hash, IRowCacheEntry value, boolean withLock) {
            int keySize = keySize(key);
            int valueSize = valueSize(value);

            if (keySize + valueSize > CapiBlockDevice.BLOCK_SIZE - 8)
                return;

            ByteBuffer bb = ByteBuffer.allocateDirect(CapiBlockDevice.BLOCK_SIZE);
            bb.putInt(keySize);
            bb.putInt(valueSize);
            serializeKey(key, bb);
            serializeValue(value, bb);

            final AtomicReference<Object> ref = new AtomicReference<Object>(null);
            if (withLock)
                lock(hash);

            try {
                sm.writeAsync(getLBA(hash) * CapiBlockDevice.BLOCK_SIZE, bb, new CapiChunkDriver.AsyncHandler() {

                    @Override
                    public void success(ByteBuffer bb) {
                        ref.set(bb);
                        synchronized (ref) {
                            ref.notify();
                        }
                    }

                    @Override
                    public void error(String msg) {
                        ref.set(msg);
                        synchronized (ref) {
                            ref.notify();
                        }
                    }
                });

                synchronized (ref) {
                    if (ref.get() == null)
                        try {
                            ref.wait();
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                }

                Object ret = ref.get();
                if (ret instanceof String) {
                    logger.error((String) ret);
                    throw new IllegalStateException((String) ret);
                }

            } catch (IOException ex) {
                logger.error(ex.getMessage(), ex);
                return;
            } finally {
                if (withLock)
                    unlock(hash);
            }
            //logger.info("put: key=" + hashFunc.toString(key.key) + ", hash=" + hash + ", lba=" + getLBA(hash));
        }

        @Override
        public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (exist(hash, false))
                    return false;

                map.put(key, value);
                putToCapi(key, hash, value, false);
                filled(hash, false);

                if (cachePush.incrementAndGet() % logUnit == 0)
                    log();

                return true;
            } finally {
                unlock(hash);
            }
        }

        @Override
        public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (!exist(hash, false))
                    return false;

                IRowCacheEntry current = map.getIfPresent(key);
                if (current == null) {
                    current = getFromCapi(key, hash);
                    if (current == null) {
                        logger.error("filter is not consistent.");
                        return false;
                    }
                }

                map.put(key, value);
                putToCapi(key, hash, value, false);
                //filled(hash, false);

                if (cachePush.incrementAndGet() % logUnit == 0)
                    log();

                return true;
            } finally {
                unlock(hash);
            }
        }

        IRowCacheEntry getFromCapi(RowCacheKey key, int hash) {
            final AtomicReference<Object> ref = new AtomicReference<Object>(null);

            try {
                sm.readAsync(getLBA(hash) * CapiBlockDevice.BLOCK_SIZE, CapiBlockDevice.BLOCK_SIZE, new CapiChunkDriver.AsyncHandler() {

                    @Override
                    public void success(ByteBuffer bb) {
                        ref.set(bb);
                        synchronized (ref) {
                            ref.notify();
                        }
                    }

                    @Override
                    public void error(String msg) {
                        ref.set(msg);
                        synchronized (ref) {
                            ref.notify();
                        }
                    }
                });
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }

            synchronized (ref) {
                if (ref.get() == null)
                    try {
                        ref.wait();
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                        throw new IllegalStateException(e.getMessage(), e);
                    }
            }

            Object ret = ref.get();
            if (ret instanceof String) {
                logger.error((String) ret);
                throw new IllegalStateException((String) ret);
            }
            ByteBuffer keyAndValueBB = (ByteBuffer) ref.get();
            int keySizeInEntry = keyAndValueBB.getInt(); // header 1
            int valueSizeInEntry = keyAndValueBB.getInt(); // header 2

            int keySize = keySize(key);
            if (keySize != keySizeInEntry) {
                logger.info("key size is different.: key=" + hashFunc.toString(key.key) + ", hash=" + hash + ", lba=" + getLBA(hash) + ", arg=" + keySize + ", cache=" + keySizeInEntry);
                return null;
            }

            ByteBuffer keyBB = ByteBuffer.allocateDirect(keySize);
            serializeKey(key, keyBB);
            keyBB.rewind();

            ByteBuffer keyBBInEntry = ((ByteBuffer) keyAndValueBB.limit(keySize + 8)).slice();
            keyBBInEntry.rewind();

            if (keyBB.equals(keyBBInEntry)) {
                keyAndValueBB.rewind().position(keySize + 8).limit(keySize + valueSizeInEntry + 8);
                ByteBuffer valueBBInEntry = keyAndValueBB.slice();
                return deserializeValue(valueBBInEntry);
            } else {
                logger.info("key value is different.: arg=" + keyBB.capacity() + ", cache=" + keyBBInEntry.capacity());
                return null;
            }
        }

        @Override
        public IRowCacheEntry get(RowCacheKey key) {

            int hash = hashFunc.hashCode(key.key);

            IRowCacheEntry entry = map.getIfPresent(key);
            if (entry == null) {
                if (exist(hash, false)) {
                    entry = getFromCapi(key, hash);
                    if (entry != null)
                        swapin.incrementAndGet();
                    else
                        swapinMiss.incrementAndGet();
                }
            }

            if (entry == null) {
                if (cacheMiss.incrementAndGet() % logUnit == 0)
                    log();
            } else {
                if (cacheHit.incrementAndGet() % logUnit == 0)
                    log();
            }
            return entry;
        }

        @Override
        public void remove(RowCacheKey key) {
            int hash = hashFunc.hashCode(key.key);
            lock(hash);
            try {
                if (!exist(hash, false))
                    return;

                invalidate(hash, false);

            } finally {
                unlock(hash);
            }
        }

        @Override
        public void clear() {
            for (ReentrantLock lock : monitors)
                lock.lock();
            try {
                for (int i = 0; i < filters.length; ++i)
                    filters[i] = 0;
            } finally {
                for (ReentrantLock lock : monitors)
                    lock.unlock();
            }
        }

        @Override
        public Iterator<RowCacheKey> keyIterator() {
            clear();
            return new Iterator<RowCacheKey>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public RowCacheKey next() {
                    return null;
                }
            };
        }

        @Override
        public Iterator<RowCacheKey> hotKeyIterator(int n) {
            return new Iterator<RowCacheKey>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public RowCacheKey next() {
                    return null;
                }
            };
        }

        @Override
        public boolean containsKey(RowCacheKey key) {
            return get(key) != null;
        }

	public void clearForTest() {
            map.cleanUp();
        }
    }

    static long logUnit = 100000L;
    static long lastLog = System.currentTimeMillis();
    static long lastCapiRead = 0L;
    static long lastCapiRowRead = 0L;

    static synchronized void log() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastLog;
        lastLog = now;

        long currentCapiRead = swapin.get() + swapinMiss.get();
        long count = currentCapiRead - lastCapiRead;
        lastCapiRead = currentCapiRead;

        long currentCapiRowRead = CapiChunkDriver.executed.get();
        long rowCount = currentCapiRowRead - lastCapiRowRead;
        lastCapiRowRead = currentCapiRowRead;

        logger.info("cache hit/miss/push : " + cacheHit + "/" + cacheMiss + "/" + cachePush + ", "//
                + "total swapped-in (success/miss/error/remove) : " + swapin + "/" + swapinMiss + "/" + swapinErr + "/" + remove + ", throughput (cache/capi): " + (count / (double) elapsed) * 1000.0 + "/" + (rowCount / (double) elapsed) * 1000.0);
    }

}
