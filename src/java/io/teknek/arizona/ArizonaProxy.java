package io.teknek.arizona;

import io.teknek.arizona.transform.FunctionalTransform;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.util.concurrent.Uninterruptibles;

public class ArizonaProxy {

  public static ColumnFamily functional_transform(String keyspaceName, String cfName,
          ByteBuffer key, FunctionalTransform transform, 
          ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit)
          throws UnavailableException, IsBootstrappingException, ReadTimeoutException,
          WriteTimeoutException, InvalidRequestException {
    consistencyForPaxos.validateForCas();
    consistencyForCommit.validateForCasCommit(keyspaceName);
    long start = System.nanoTime();
    long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
    while (System.nanoTime() - start < timeout)
    {
        Pair<List<InetAddress>, Integer> p = StorageProxy.instance.getPaxosParticipants(keyspaceName, key, consistencyForPaxos);
        List<InetAddress> liveEndpoints = p.left;
        int requiredParticipants = p.right;
        UUID ballot = StorageProxy.beginAndRepairPaxos(start, key, transform.getCfm(), liveEndpoints, requiredParticipants, consistencyForPaxos);
        long timestamp = System.currentTimeMillis();
        ColumnParent cp = new ColumnParent(cfName);
        IDiskAtomFilter filter = CassandraServer.toInternalFilter(transform.getCfm(), cp, transform.getSlicePredicate());
        ReadCommand readCommand = ReadCommand.create(keyspaceName, key, cfName, timestamp, filter);
        List<Row> rows = StorageProxy.read(Arrays.asList(readCommand), consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM);
        ColumnFamily updates = transform.getTransformer().transform(rows.get(0).cf, new HashMap(), transform.getCfm());
        Commit proposal = Commit.newProposal(key, ballot, updates);
        if (StorageProxy.proposePaxos(proposal, liveEndpoints, requiredParticipants, true))
        {
            if (consistencyForCommit == ConsistencyLevel.ANY)
              StorageProxy.sendCommit(proposal, liveEndpoints);
            else
              StorageProxy.commitPaxos(proposal, consistencyForCommit);
            Tracing.trace("CAS successful");
            return updates;
        }
        Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
        Uninterruptibles.sleepUninterruptibly(FBUtilities.threadLocalRandom().nextInt(100), TimeUnit.MILLISECONDS);
    }
    throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
  }
}

                     








