/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.streaming.StreamingChannel.Factory.Global.streamingFactory;

/**
 * Cassandra SSTable bulk loader.
 * Load an externally created sstable into a cluster.
 */
public class SSTableLoader implements StreamEventHandler
{
    private final File directory;
    private final String keyspace;
    private final Client client;
    private final int connectionsPerHost;
    private final OutputHandler outputHandler;
    private final Set<InetAddressAndPort> failedHosts = new HashSet<>();

    private final List<SSTableReader> sstables = new ArrayList<>();
    private final Multimap<InetAddressAndPort, OutgoingStream> streamingDetails = HashMultimap.create();

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler)
    {
        this(directory, client, outputHandler, 1, null);
    }

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler, int connectionsPerHost, String targetKeyspace)
    {
        this.directory = directory;
        this.keyspace = targetKeyspace != null ? targetKeyspace : directory.parent().name();
        this.client = client;
        this.outputHandler = outputHandler;
        this.connectionsPerHost = connectionsPerHost;
    }

    @SuppressWarnings("resource")
    protected Collection<SSTableReader> openSSTables(final Map<InetAddressAndPort, Collection<Range<Token>>> ranges)
    {
        outputHandler.output("Opening sstables and calculating sections to stream");

        LifecycleTransaction.getFiles(directory.toPath(),
                                      (file, type) ->
                                      {
                                          File dir = file.parent();
                                          String name = file.name();

                                          if (type != Directories.FileType.FINAL)
                                          {
                                              outputHandler.output(String.format("Skipping temporary file %s", name));
                                              return false;
                                          }

                                          Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(file);
                                          Descriptor desc = p == null ? null : p.left;
                                          if (p == null || !p.right.equals(Component.DATA))
                                              return false;

                                          if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists())
                                          {
                                              outputHandler.output(String.format("Skipping file %s because index is missing", name));
                                              return false;
                                          }

                                          TableMetadataRef metadata = client.getTableMetadata(desc.cfname);
                                          if (metadata == null)
                                          {
                                              outputHandler.output(String.format("Skipping file %s: table %s.%s doesn't exist", name, keyspace, desc.cfname));
                                              return false;
                                          }

                                          Set<Component> components = new HashSet<>();
                                          components.add(Component.DATA);
                                          components.add(Component.PRIMARY_INDEX);
                                          if (new File(desc.filenameFor(Component.SUMMARY)).exists())
                                              components.add(Component.SUMMARY);
                                          if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists())
                                              components.add(Component.COMPRESSION_INFO);
                                          if (new File(desc.filenameFor(Component.STATS)).exists())
                                              components.add(Component.STATS);

                                          try
                                          {
                                              // To conserve memory, open SSTableReaders without bloom filters and discard
                                              // the index summary after calculating the file sections to stream and the estimated
                                              // number of keys for each endpoint. See CASSANDRA-5555 for details.
                                              SSTableReader sstable = SSTableReader.openForBatch(desc, components, metadata);
                                              sstables.add(sstable);

                                              // calculate the sstable sections to stream as well as the estimated number of
                                              // keys per host
                                              for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : ranges.entrySet())
                                              {
                                                  InetAddressAndPort endpoint = entry.getKey();
                                                  List<Range<Token>> tokenRanges = Range.normalize(entry.getValue());

                                                  List<SSTableReader.PartitionPositionBounds> sstableSections = sstable.getPositionsForRanges(tokenRanges);
                                                  // Do not stream to nodes that don't own any part of the SSTable, empty streams
                                                  // will generate an error on the server. See CASSANDRA-16349 for details.
                                                  if (sstableSections.isEmpty())
                                                      continue;

                                                  long estimatedKeys = sstable.estimatedKeysForRanges(tokenRanges);
                                                  Ref<SSTableReader> ref = sstable.ref();
                                                  OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.BULK_LOAD, ref, sstableSections, tokenRanges, estimatedKeys);
                                                  streamingDetails.put(endpoint, stream);
                                              }

                                              // to conserve heap space when bulk loading
                                              sstable.releaseSummary();
                                          }
                                          catch (FSError e)
                                          {
                                              // todo: should we really continue if we can't open all sstables?
                                              outputHandler.output(String.format("Skipping file %s, error opening it: %s", name, e.getMessage()));
                                          }
                                          return false;
                                      },
                                      Directories.OnTxnErr.IGNORE);

        return sstables;
    }

    public StreamResultFuture stream()
    {
        return stream(Collections.<InetAddressAndPort>emptySet());
    }

    public StreamResultFuture stream(Set<InetAddressAndPort> toIgnore, StreamEventHandler... listeners)
    {
        client.init(keyspace);
        outputHandler.output("Established connection to initial hosts");

        StreamPlan plan = new StreamPlan(StreamOperation.BULK_LOAD, connectionsPerHost, false, null, PreviewKind.NONE).connectionFactory(client.getConnectionFactory());

        Map<InetAddressAndPort, Collection<Range<Token>>> endpointToRanges = client.getEndpointToRangesMap();
        openSSTables(endpointToRanges);
        if (sstables.isEmpty())
        {
            // return empty result
            return plan.execute();
        }

        outputHandler.output(String.format("Streaming relevant part of %s to %s", names(sstables), endpointToRanges.keySet()));

        for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : endpointToRanges.entrySet())
        {
            InetAddressAndPort remote = entry.getKey();
            if (toIgnore.contains(remote))
                continue;

            List<OutgoingStream> streams = new LinkedList<>();

            // references are acquired when constructing the SSTableStreamingSections above
            for (OutgoingStream stream : streamingDetails.get(remote))
            {
                streams.add(stream);
            }

            plan.transferStreams(remote, streams);
        }
        plan.listeners(this, listeners);
        return plan.execute();
    }

    public void onSuccess(StreamState finalState)
    {
        releaseReferences();
    }
    public void onFailure(Throwable t)
    {
        releaseReferences();
    }

    /**
     * releases the shared reference for all sstables, we acquire this when opening the sstable
     */
    private void releaseReferences()
    {
        for (SSTableReader sstable : sstables)
        {
            sstable.selfRef().release();
            assert sstable.selfRef().globalCount() == 0 : String.format("for sstable = %s, ref count = %d", sstable, sstable.selfRef().globalCount());
        }
    }

    public void handleStreamEvent(StreamEvent event)
    {
        if (event.eventType == StreamEvent.Type.STREAM_COMPLETE)
        {
            StreamEvent.SessionCompleteEvent se = (StreamEvent.SessionCompleteEvent) event;
            if (!se.success)
                failedHosts.add(se.peer);
        }
    }

    private String names(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
            builder.append(sstable.descriptor.filenameFor(Component.DATA)).append(" ");
        return builder.toString();
    }

    public Set<InetAddressAndPort> getFailedHosts()
    {
        return failedHosts;
    }

    public static abstract class Client
    {
        private final Map<InetAddressAndPort, Collection<Range<Token>>> endpointToRanges = new HashMap<>();

        /**
         * Initialize the client.
         * Perform any step necessary so that after the call to the this
         * method:
         *   * partitioner is initialized
         *   * getEndpointToRangesMap() returns a correct map
         * This method is guaranteed to be called before any other method of a
         * client.
         */
        public abstract void init(String keyspace);

        /**
         * Stop the client.
         */
        public void stop()
        {
        }

        /**
         * Provides connection factory.
         * By default, it uses DefaultConnectionFactory.
         *
         * @return StreamConnectionFactory to use
         */
        public StreamingChannel.Factory getConnectionFactory()
        {
            return streamingFactory();
        }

        /**
         * Validate that {@code keyspace} is an existing keyspace and {@code
         * cfName} one of its existing column family.
         */
        public abstract TableMetadataRef getTableMetadata(String tableName);

        public void setTableMetadata(TableMetadataRef cfm)
        {
            throw new RuntimeException();
        }

        public Map<InetAddressAndPort, Collection<Range<Token>>> getEndpointToRangesMap()
        {
            return endpointToRanges;
        }

        protected void addRangeForEndpoint(Range<Token> range, InetAddressAndPort endpoint)
        {
            Collection<Range<Token>> ranges = endpointToRanges.get(endpoint);
            if (ranges == null)
            {
                ranges = new HashSet<>();
                endpointToRanges.put(endpoint, ranges);
            }
            ranges.add(range);
        }
    }
}
