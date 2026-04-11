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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamingChannel;
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
    private final String table;
    private final Client client;
    private final int connectionsPerHost;
    private final OutputHandler outputHandler;
    private final Set<InetAddressAndPort> failedHosts = new HashSet<>();

    private final List<SSTableReader> sstables = new ArrayList<>();

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler)
    {
        this(directory, client, outputHandler, 1, null);
    }

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler, int connectionsPerHost, String targetKeyspace)
    {
        this(directory, client, outputHandler, connectionsPerHost, targetKeyspace, null);
    }

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler, int connectionsPerHost, String targetKeyspace, String targetTable)
    {
        this.directory = directory;
        this.keyspace = targetKeyspace != null ? targetKeyspace : directory.parent().name();
        this.table = targetTable;
        this.client = client;
        this.outputHandler = outputHandler;
        this.connectionsPerHost = connectionsPerHost;
    }

    private Multimap<InetAddressAndPort, CassandraOutgoingFile> openSSTables(final Map<InetAddressAndPort, Collection<Range<Token>>> ranges)
    {
        outputHandler.output("Opening sstables and calculating sections to stream");

        Multimap<InetAddressAndPort, CassandraOutgoingFile> streamingDetails = HashMultimap.create();
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

                                          Pair<Descriptor, Component> p;
                                          if (null != keyspace && null != table)
                                          {
                                              p = SSTable.tryComponentFromFilename(file, keyspace, table);
                                          }
                                          else
                                          {
                                              p = SSTable.tryComponentFromFilename(file);
                                          }

                                          Descriptor desc = p == null ? null : p.left;
                                          if (p == null || !p.right.equals(Components.DATA))
                                              return false;

                                          for (Component c : desc.getFormat().primaryComponents())
                                          {
                                              if (!desc.fileFor(c).exists())
                                              {
                                                  outputHandler.output(String.format("Skipping file %s because %s is missing", name, c.name));
                                                  return false;
                                              }
                                          }

                                          TableMetadataRef metadata = client.getTableMetadata(desc.cfname);
                                          if (metadata == null)
                                          {
                                              outputHandler.output(String.format("Skipping file %s: table %s.%s doesn't exist", name, keyspace, desc.cfname));
                                              return false;
                                          }

                                          Set<Component> components = desc.getComponents(desc.getFormat().primaryComponents(), desc.getFormat().uploadComponents());

                                          try
                                          {
                                              // To conserve memory, open SSTableReaders without bloom filters and discard
                                              // the index summary after calculating the file sections to stream and the estimated
                                              // number of keys for each endpoint. See CASSANDRA-5555 for details.
                                              SSTableReader sstable = SSTableReader.openForBatch(null, desc, components, metadata);
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
                                                  CassandraOutgoingFile stream = new CassandraOutgoingFile(StreamOperation.BULK_LOAD, ref, sstableSections, tokenRanges, estimatedKeys);
                                                  streamingDetails.put(endpoint, stream);
                                              }

                                              // to conserve heap space when bulk loading
                                              sstable.releaseInMemoryComponents();
                                          }
                                          catch (FSError e)
                                          {
                                              // todo: should we really continue if we can't open all sstables?
                                              outputHandler.output(String.format("Skipping file %s, error opening it: %s", name, e.getMessage()));
                                          }
                                          return false;
                                      },
                                      Directories.OnTxnErr.IGNORE);

        return streamingDetails;
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
        Multimap<InetAddressAndPort, CassandraOutgoingFile> streamingDetails = openSSTables(endpointToRanges);
        if (streamingDetails.isEmpty())
        {
            // return empty result
            return plan.execute();
        }

        outputHandler.output(String.format("Streaming relevant part of %s to %s", names(streamingDetails.values()), endpointToRanges.keySet()));

        for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : endpointToRanges.entrySet())
        {
            InetAddressAndPort remote = entry.getKey();
            if (toIgnore.contains(remote))
                continue;

            // references are acquired when constructing the SSTableStreamingSections above
            List<OutgoingStream> streams = new LinkedList<>(streamingDetails.get(remote));

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
        Iterator<SSTableReader> it = sstables.iterator();
        while (it.hasNext())
        {
            SSTableReader sstable = it.next();
            sstable.selfRef().release();
            it.remove();
        }
    }

    @VisibleForTesting
    ImmutableList<SSTableReader> getSSTables()
    {
        return ImmutableList.copyOf(sstables);
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

    private String names(Collection<CassandraOutgoingFile> sstables)
    {
        return sstables.stream().map(CassandraOutgoingFile::getName).distinct().collect(Collectors.joining(" "));
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
