package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cache.capi.CapiChunkDriver.AsyncHandler;

import com.ibm.research.capiblock.CapiBlockDevice;

public class SimpleCapiSpaceManager {
    public static class OutOfStorageException extends IOException {
        private static final long serialVersionUID = 1L;

        public OutOfStorageException(String msg) {
            super(msg);
        }
    }

    private List<CapiChunkDriver> drivers = new ArrayList<>();
    private List<Long> driver2StartLBA = new ArrayList<>();
    private long lbaSizeForEach = 0;
    private boolean initialized = false; // must be volatile for safty
    private boolean closed = false;// must be volatile for safety
    private long limit;

    public SimpleCapiSpaceManager() {
    }

    public void add(CapiChunkDriver driver, long startLba, long lbaSize) {
        assert (driver != null);
        assert (startLba >= 0);
        assert (lbaSize > 0);

        if (initialized)
            throw new IllegalStateException("already initialized.");

        drivers.add(driver);
        driver2StartLBA.add(startLba);

        if (lbaSizeForEach == 0L)
            lbaSizeForEach = lbaSize;
        else
            lbaSizeForEach = Math.min(lbaSizeForEach, lbaSize);
    }

    public void initialize() throws IOException {
        initialize(true);
    }

    public synchronized void initialize(boolean clean) throws IOException {
        if (initialized)
            throw new IllegalStateException("already initialized.");

        limit = (long) drivers.size() * (long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE;

        initialized = true;

    }

    public void flush() throws IOException {
        checkActive();

        for (CapiChunkDriver driver : drivers)
            driver.flush();
    }

    public synchronized void close() throws IOException {
        if (closed)
            return;

        for (CapiChunkDriver driver : drivers)
            driver.flush();

        for (CapiChunkDriver driver : drivers)
            driver.close();

        closed = true;
    }

    int getNumberOfDevices() {
        return drivers.size();
    }

    private void checkActive() {
        if (!initialized)
            throw new IllegalStateException("not initialized.");
        if (closed)
            throw new IllegalStateException("already closed.");
    }

    long getLimitForEachDriver() {
        checkActive();
        return lbaSizeForEach;
    }

    public long getLimitInBytes() {
        checkActive();
        return limit;
    }

    public long getLimitInBlocks() {
        checkActive();
        return limit / CapiBlockDevice.BLOCK_SIZE;
    }

    class RealAddressRange {
        CapiChunkDriver driver;
        long startLBA;
        long endLBA;

        int numOfReads = 0;
        int numOfWrites = 0;

        RealAddressRange prev = null;
        RealAddressRange next = null;

        boolean isOverlapedWith(RealAddressRange other) {
            if (driver != other.driver)
                return false;
            if (startLBA < other.startLBA && other.startLBA < endLBA)
                return true;
            if (startLBA < other.endLBA && other.endLBA < endLBA)
                return true;
            if (other.startLBA < startLBA && endLBA < other.endLBA)
                return true;
            return false;
        }

        public String toString() {
            return "[ " + driver + ":" + startLBA + "-" + endLBA + "]";
        }

        public int size() {
            return (int) (endLBA - startLBA) * CapiBlockDevice.BLOCK_SIZE;
        }

        ByteBuffer read() throws IOException {
            int numOfBlocks = (int) (endLBA - startLBA);
            ByteBuffer bb = driver.read(startLBA, numOfBlocks);
            return bb;
        }

        void readAsync(final AsyncHandler handler) throws IOException {
            int numOfBlocks = (int) (endLBA - startLBA);
            driver.readAsync(startLBA, numOfBlocks, handler);
        }

        void write(ByteBuffer bb) throws IOException {
            driver.write(startLBA, bb);
        }

        void writeAsync(final ByteBuffer bb, final AsyncHandler handler) throws IOException {
            driver.writeAsync(startLBA, bb, handler);
        }
    }

    private void checkAddressAlignment(long address) {
        if (address % (long) CapiBlockDevice.BLOCK_SIZE != 0L)
            throw new IllegalArgumentException("address is not aligned.");
    }

    private void checkSize(long size) {
        if (size <= 0L)
            throw new IllegalArgumentException("size must be positive: size=" + size);
        if (size >= (long) Integer.MAX_VALUE)
            throw new IllegalArgumentException("size must be in Integer.MAX_VALUE.");
    }

    private void checkRange(long address, long sizeOfBytesInt) {
        int startDeviceIdx = (int) (address / ((long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE));
        int endDeviceIdx = (int) ((address + sizeOfBytesInt - 1) / ((long) lbaSizeForEach * (long) CapiBlockDevice.BLOCK_SIZE));

        if (startDeviceIdx != endDeviceIdx)
            throw new IllegalArgumentException("inter-driver allocation is not supportted in the current version.");
    }

    RealAddressRange getRealAddressRange(long address, int sizeOfBytesInt) {
        if (address > limit)
            throw new IllegalArgumentException("address is too large");
        if (address < 0)
            throw new IllegalArgumentException("address is negative");

        RealAddressRange real = new RealAddressRange();

        int idx = (int) (address / ((long) CapiBlockDevice.BLOCK_SIZE * (long) lbaSizeForEach));
        real.driver = drivers.get(idx);
        long startLBA = driver2StartLBA.get(idx);

        long addressInDriver = address % ((long) CapiBlockDevice.BLOCK_SIZE * (long) lbaSizeForEach);

        real.startLBA = (int) (addressInDriver / (long) CapiBlockDevice.BLOCK_SIZE) + startLBA;

        real.endLBA = (int) ((addressInDriver + (long) sizeOfBytesInt) / (long) CapiBlockDevice.BLOCK_SIZE) + startLBA;

        return real;
    }

    public void readAsync(long address, long sizeOfBytes, AsyncHandler handler) throws IOException {
        checkActive();
        checkSize(sizeOfBytes);
        checkAddressAlignment(address);
        checkAddressAlignment(address + sizeOfBytes);
        checkRange(address, sizeOfBytes);

        if (sizeOfBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("each block must be less than " + Integer.MAX_VALUE);

        int sizeOfBytesInt = (int) sizeOfBytes;

        RealAddressRange range = getRealAddressRange(address, sizeOfBytesInt);
        range.readAsync(handler);
    }

    public void writeAsync(long address, ByteBuffer bb, AsyncHandler handler) throws IOException {
        checkActive();
        checkAddressAlignment(address);
        checkAddressAlignment(address + bb.capacity());
        checkRange(address, bb.capacity());
        //System.out.println("write: " + address + "-" + (address + bb.capacity()));

        RealAddressRange range = getRealAddressRange(address, bb.capacity());
        range.writeAsync(bb, handler);

    }

}
