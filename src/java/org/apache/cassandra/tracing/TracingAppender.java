package org.apache.cassandra.tracing;

import static org.apache.cassandra.tracing.Tracing.*;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class TracingAppender extends AppenderSkeleton
{
    protected void append(final LoggingEvent event)
    {
        if (Tracing.instance() == null) // instance might not be built at the time this is called
            return;
        
        final TraceState state = Tracing.instance().get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        final int elapsed = state.elapsed();
        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow() throws TimedOutException, UnavailableException
            {
                ByteBuffer eventId = ByteBufferUtil.bytes(UUIDGen.makeType1UUIDFromHost(FBUtilities
                        .getBroadcastAddress()));
                CFMetaData cfMeta = CFMetaData.TraceEventsCf;
                ColumnFamily cf = ColumnFamily.create(cfMeta);
                addColumn(cf, buildName(cfMeta, eventId, bytes("source")), FBUtilities.getBroadcastAddress());
                addColumn(cf, buildName(cfMeta, eventId, bytes("thread")), event.getThreadName());
                addColumn(cf, buildName(cfMeta, eventId, bytes("happened_at")), event.getTimeStamp());
                addColumn(cf, buildName(cfMeta, eventId, bytes("source_elapsed")), elapsed);
                addColumn(cf, buildName(cfMeta, eventId, bytes("activity")), event.getMessage());
                RowMutation mutation = new RowMutation(Tracing.TRACE_KS, state.sessionIdBytes);
                mutation.add(cf);
                StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
            }
        });
    }

    public void close()
    {
    }

    public boolean requiresLayout()
    {
        return false;
    }
}
