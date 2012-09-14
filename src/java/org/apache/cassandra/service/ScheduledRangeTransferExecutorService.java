package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

public class ScheduledRangeTransferExecutorService
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledRangeTransferExecutorService.class);
    private static final int INTERVAL = 10;
    private ScheduledExecutorService scheduler;

    public void setup()
    {
        if (DatabaseDescriptor.getNumTokens() == 1)
        {
            LOG.warn("Cannot start range transfer scheduler: endpoint is not virtual nodes-enabled");
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(new RangeTransferThreadFactory());
        scheduler.scheduleWithFixedDelay(new RangeTransfer(), 0, INTERVAL, TimeUnit.SECONDS);
        LOG.info("Enabling scheduled transfers of token ranges");
    }

    public void tearDown()
    {
        if (scheduler == null)
        {
            LOG.warn("Unabled to shutdown; Scheduler never enabled");
            return;
        }
 
        LOG.info("Shutting down range transfer scheduler");
        scheduler.shutdownNow();
    }
}

class RangeTransfer implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(RangeTransfer.class);

    public void run()
    {
        UntypedResultSet res = processInternal("SELECT * FROM system." + SystemTable.RANGE_XFERS_CF + " LIMIT 1");

        if (res.size() < 1)
        {
            LOG.debug("No queued ranges to transfer");
            return;
        }

        if (!isReady())
            return;

        UntypedResultSet.Row row = res.iterator().next();

        Date requestedAt = row.getTimestamp("requested_at");
        ByteBuffer tokenBytes = row.getBytes("token_bytes");
        Token token = StorageService.getPartitioner().getTokenFactory().fromByteArray(tokenBytes);

        LOG.info("Initiating transfer of {} (scheduled at {})", token, requestedAt.toString());
        try
        {
            StorageService.instance.relocateTokens(Collections.singleton(token));
        }
        catch (Exception e)
        {
            LOG.error("Error removing {}: {}", token, e);
        }
        finally
        {
            LOG.debug("Removing queued entry for transfer of {}", token);
            processInternal(String.format("DELETE FROM system.%s WHERE token_bytes = '%s'",
                                          SystemTable.RANGE_XFERS_CF,
                                          ByteBufferUtil.bytesToHex(tokenBytes)));
        }
    }

    private boolean isReady()
    {
        int targetTokens = DatabaseDescriptor.getNumTokens();
        int highMark = (int)Math.ceil(targetTokens + (targetTokens * .10));
        int actualTokens = StorageService.instance.getTokens().size();

        if (actualTokens >= highMark)
        {
            LOG.warn("Pausing until token count stabilizes (target={}, actual={})", targetTokens, actualTokens);
            return false;
        }

        return true;
    }
}

class RangeTransferThreadFactory implements ThreadFactory
{
    private AtomicInteger count = new AtomicInteger(0);

    public Thread newThread(Runnable r)
    {
        Thread rangeXferThread = new Thread(r);
        rangeXferThread.setName(String.format("ScheduledRangeXfers:%d", count.getAndIncrement()));
        return rangeXferThread;
    }
}
