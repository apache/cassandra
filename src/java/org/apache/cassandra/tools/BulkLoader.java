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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.shaded.netty.channel.socket.SocketChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class BulkLoader
{
    public static void main(String[] args) throws BulkLoadException
    {
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        load(options);
    }

    public static void load(LoaderOptions options) throws BulkLoadException
    {
        DatabaseDescriptor.toolInitialization();
        OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
        SSTableLoader loader = new SSTableLoader(
                options.directory.toAbsolute(),
                new ExternalClient(
                        options.hosts,
                        options.storagePort,
                        options.authProvider,
                        options.serverEncOptions,
                        buildSSLOptions(options.clientEncOptions)),
                        handler,
                        options.connectionsPerHost,
                        options.targetKeyspace,
                        options.targetTable);
        DatabaseDescriptor.setStreamThroughputOutboundBytesPerSec(options.throttleBytes);
        DatabaseDescriptor.setInterDCStreamThroughputOutboundBytesPerSec(options.interDcThrottleBytes);
        DatabaseDescriptor.setEntireSSTableStreamThroughputOutboundMebibytesPerSec(options.entireSSTableThrottleMebibytes);
        DatabaseDescriptor.setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(options.entireSSTableInterDcThrottleMebibytes);
        StreamResultFuture future;

        ProgressIndicator indicator = new ProgressIndicator();
        try
        {
            if (options.noProgress)
            {
                future = loader.stream(options.ignores);
            }
            else
            {
                future = loader.stream(options.ignores, indicator);
            }
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            System.err.println(e.getMessage());
            if (e.getCause() != null)
            {
                System.err.println(e.getCause());
            }
            e.printStackTrace(System.err);
            throw new BulkLoadException(e);
        }

        try
        {
            future.get();

            if (!options.noProgress)
            {
                indicator.printSummary(options.connectionsPerHost);
            }

            // Give sockets time to gracefully close
            Thread.sleep(1000);
        }
        catch (Exception e)
        {
            System.err.println("Streaming to the following hosts failed:");
            System.err.println(loader.getFailedHosts());
            e.printStackTrace(System.err);
            throw new BulkLoadException(e);
        }
    }

    // Return true when everything is at 100%
    static class ProgressIndicator implements StreamEventHandler
    {
        private final long start;
        private long lastProgress;
        private long lastTime;

        private long peak = 0;
        private int totalFiles = 0;

        private final Multimap<InetSocketAddress, SessionInfo> sessionsByHost = HashMultimap.create();

        public ProgressIndicator()
        {
            start = lastTime = nanoTime();
        }

        public void onSuccess(StreamState finalState)
        {
        }

        public void onFailure(Throwable t)
        {
        }

        public synchronized void handleStreamEvent(StreamEvent event)
        {
            if (event.eventType == StreamEvent.Type.STREAM_PREPARED)
            {
                SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
                sessionsByHost.put(session.peer, session);
            }
            else if (event.eventType == StreamEvent.Type.FILE_PROGRESS || event.eventType == StreamEvent.Type.STREAM_COMPLETE)
            {
                ProgressInfo progressInfo = null;
                if (event.eventType == StreamEvent.Type.FILE_PROGRESS)
                {
                    progressInfo = ((StreamEvent.ProgressEvent) event).progress;
                }

                long time = nanoTime();
                long deltaTime = time - lastTime;

                StringBuilder sb = new StringBuilder();
                sb.append("\rprogress: ");

                long totalProgress = 0;
                long totalSize = 0;

                boolean updateTotalFiles = totalFiles == 0;
                // recalculate progress across all sessions in all hosts and display
                for (InetSocketAddress peer : sessionsByHost.keySet())
                {
                    sb.append("[").append(peer).append("]");

                    for (SessionInfo session : sessionsByHost.get(peer))
                    {
                        long size = session.getTotalSizeToSend();
                        long current = 0;
                        int completed = 0;

                        if (progressInfo != null && session.peer.equals(progressInfo.peer) && session.sessionIndex == progressInfo.sessionIndex)
                        {
                            session.updateProgress(progressInfo);
                        }
                        for (ProgressInfo progress : session.getSendingFiles())
                        {
                            if (progress.isCompleted())
                            {
                                completed++;
                            }
                            current += progress.currentBytes;
                        }
                        totalProgress += current;

                        totalSize += size;

                        sb.append(session.sessionIndex).append(":");
                        sb.append(completed).append("/").append(session.getTotalFilesToSend());
                        sb.append(" ").append(String.format("%-3d", size == 0 ? 100L : current * 100L / size)).append("% ");

                        if (updateTotalFiles)
                        {
                            totalFiles += session.getTotalFilesToSend();
                        }
                    }
                }

                lastTime = time;
                long deltaProgress = totalProgress - lastProgress;
                lastProgress = totalProgress;

                sb.append("total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append("% ");
                sb.append(FBUtilities.prettyPrintMemoryPerSecond(deltaProgress, deltaTime));
                long average = bytesPerSecond(totalProgress, time - start);

                if (average > peak)
                {
                    peak = average;
                }
                sb.append(" (avg: ").append(FBUtilities.prettyPrintMemoryPerSecond(totalProgress, time - start)).append(")");

                System.out.println(sb);
            }
        }

        private long bytesPerSecond(long bytes, long timeInNano)
        {
            return timeInNano != 0 ? (long) (((double) bytes / timeInNano) * 1000 * 1000 * 1000) : 0;
        }

        private void printSummary(int connectionsPerHost)
        {
            long end = nanoTime();
            long durationMS = ((end - start) / (1000000));

            StringBuilder sb = new StringBuilder();
            sb.append("\nSummary statistics: \n");
            sb.append(String.format("   %-24s: %-10d%n", "Connections per host ", connectionsPerHost));
            sb.append(String.format("   %-24s: %-10d%n", "Total files transferred ", totalFiles));
            sb.append(String.format("   %-24s: %-10s%n", "Total bytes transferred ", FBUtilities.prettyPrintMemory(lastProgress)));
            sb.append(String.format("   %-24s: %-10s%n", "Total duration ", durationMS + " ms"));
            sb.append(String.format("   %-24s: %-10s%n", "Average transfer rate ", FBUtilities.prettyPrintMemoryPerSecond(lastProgress, end - start)));
            sb.append(String.format("   %-24s: %-10s%n", "Peak transfer rate ",  FBUtilities.prettyPrintMemoryPerSecond(peak)));
            System.out.println(sb);
        }
    }

    private static SSLOptions buildSSLOptions(EncryptionOptions clientEncryptionOptions)
    {

        if (!clientEncryptionOptions.getEnabled())
        {
            return null;
        }

        SSLContext sslContext;
        try
        {
            sslContext = SSLFactory.createSSLContext(clientEncryptionOptions, true);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not create SSL Context.", e);
        }

        // Temporarily override newSSLEngine to set accepted protocols until it is added to
        // RemoteEndpointAwareJdkSSLOptions.  See CASSANDRA-13325 and CASSANDRA-16362.
        RemoteEndpointAwareJdkSSLOptions sslOptions = new RemoteEndpointAwareJdkSSLOptions(sslContext, clientEncryptionOptions.cipherSuitesArray())
        {
            @Override
            protected SSLEngine newSSLEngine(SocketChannel channel, InetSocketAddress remoteEndpoint)
            {
                SSLEngine engine = super.newSSLEngine(channel, remoteEndpoint);

                String[] acceptedProtocols = clientEncryptionOptions.acceptedProtocolsArray();
                if (acceptedProtocols != null && acceptedProtocols.length > 0)
                    engine.setEnabledProtocols(acceptedProtocols);

                return engine;
            }
        };
        return sslOptions;
    }

    static class ExternalClient extends NativeSSTableLoaderClient
    {
        private final int storagePort;
        private final EncryptionOptions.ServerEncryptionOptions serverEncOptions;

        public ExternalClient(Set<InetSocketAddress> hosts,
                              int storagePort,
                              AuthProvider authProvider,
                              EncryptionOptions.ServerEncryptionOptions serverEncryptionOptions,
                              SSLOptions sslOptions)
        {
            super(hosts, storagePort, authProvider, sslOptions);
            serverEncOptions = serverEncryptionOptions;
            this.storagePort = storagePort;
        }

        @Override
        public StreamingChannel.Factory getConnectionFactory()
        {
            return new BulkLoadConnectionFactory(serverEncOptions, storagePort);
        }
    }

    public static class CmdLineOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option with argument and argument name that accepts being defined multiple times as a list
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOptionList(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);
            option.setArgs(Option.UNLIMITED_VALUES);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }
}
