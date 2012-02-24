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
package org.apache.cassandra.thrift;

import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


/**
 * Slightly modified version of the Apache Thrift TThreadPoolServer.
 * <p/>
 * This allows passing an executor so you have more control over the actual
 * behaviour of the tasks being run.
 * <p/>
 * Newer version of Thrift should make this obsolete.
 */
public class CustomTThreadPoolServer extends TServer
{

    private static final Logger logger = LoggerFactory.getLogger(CustomTThreadPoolServer.class.getName());

    // Executor service for handling client connections
    private final ExecutorService executorService;

    // Flag for stopping the server
    private volatile boolean stopped;

    // Server options
    private final TThreadPoolServer.Args args;

    //Track and Limit the number of connected clients
    private final AtomicInteger activeClients = new AtomicInteger(0);


    public CustomTThreadPoolServer(TThreadPoolServer.Args args, ExecutorService executorService) {
        super(args);
        this.executorService = executorService;
        this.args = args;
    }

    public void serve()
    {
        try
        {
            serverTransport_.listen();
        }
        catch (TTransportException ttx)
        {
            logger.error("Error occurred during listening.", ttx);
            return;
        }

        stopped = false;
        while (!stopped)
        {
            // block until we are under max clients
            while (activeClients.get() >= args.maxWorkerThreads)
            {
                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }

            try
            {
                TTransport client = serverTransport_.accept();
                activeClients.incrementAndGet();
                WorkerProcess wp = new WorkerProcess(client);
                executorService.execute(wp);
            }
            catch (TTransportException ttx)
            {
                if (ttx.getCause() instanceof SocketTimeoutException) // thrift sucks
                    continue;

                if (!stopped)
                {
                    logger.warn("Transport error occurred during acceptance of message.", ttx);
                }
            }

            if (activeClients.get() >= args.maxWorkerThreads)
                logger.warn("Maximum number of clients " + args.maxWorkerThreads + " reached");
        }

        executorService.shutdown();
        // Thrift's default shutdown waits for the WorkerProcess threads to complete.  We do not,
        // because doing that allows a client to hold our shutdown "hostage" by simply not sending
        // another message after stop is called (since process will block indefinitely trying to read
        // the next meessage header).
        //
        // The "right" fix would be to update thrift to set a socket timeout on client connections
        // (and tolerate unintentional timeouts until stopped is set).  But this requires deep
        // changes to the code generator, so simply setting these threads to daemon (in our custom
        // CleaningThreadPool) and ignoring them after shutdown is good enough.
        //
        // Remember, our goal on shutdown is not necessarily that each client request we receive
        // gets answered first [to do that, you should redirect clients to a different coordinator
        // first], but rather (1) to make sure that for each update we ack as successful, we generate
        // hints for any non-responsive replicas, and (2) to make sure that we quickly stop
        // accepting client connections so shutdown can continue.  Not waiting for the WorkerProcess
        // threads here accomplishes (2); MessagingService's shutdown method takes care of (1).
        //
        // See CASSANDRA-3335 and CASSANDRA-3727.
    }

    public void stop()
    {
        stopped = true;
        serverTransport_.interrupt();
    }

    private class WorkerProcess implements Runnable
    {

        /**
         * Client that this services.
         */
        private TTransport client_;

        /**
         * Default constructor.
         *
         * @param client Transport to process
         */
        private WorkerProcess(TTransport client)
        {
            client_ = client;
        }

        /**
         * Loops on processing a client forever
         */
        public void run()
        {
            TProcessor processor = null;
            TTransport inputTransport = null;
            TTransport outputTransport = null;
            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;
            try
            {
                processor = processorFactory_.getProcessor(client_);
                inputTransport = inputTransportFactory_.getTransport(client_);
                outputTransport = outputTransportFactory_.getTransport(client_);
                inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
                // we check stopped first to make sure we're not supposed to be shutting
                // down. this is necessary for graceful shutdown.  (but not sufficient,
                // since process() can take arbitrarily long waiting for client input.
                // See comments at the end of serve().)
                while (!stopped && processor.process(inputProtocol, outputProtocol))
                {
                    inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                    outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
                }
            }
            catch (TTransportException ttx)
            {
                // Assume the client died and continue silently
                // Log at debug to allow debugging of "frame too large" errors (see CASSANDRA-3142).
                logger.debug("Thrift transport error occurred during processing of message.", ttx);
            }
            catch (TException tx)
            {
                logger.error("Thrift error occurred during processing of message.", tx);
            }
            catch (Exception x)
            {
                logger.error("Error occurred during processing of message.", x);
            }
            finally
            {
                activeClients.decrementAndGet();
            }

            if (inputTransport != null)
            {
                inputTransport.close();
            }

            if (outputTransport != null)
            {
                outputTransport.close();
            }
        }
    }
}
