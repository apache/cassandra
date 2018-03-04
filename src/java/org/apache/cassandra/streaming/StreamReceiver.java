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

package org.apache.cassandra.streaming;

/**
 * StreamReceiver acts as a staging area for incoming data. Received data
 * ends up here, and is kept separate from the live data until all streams
 * for a session have been received successfully
 */
public interface StreamReceiver
{
    /**
     * Called after we've finished receiving stream data. The data covered by the given stream should
     * be kept isolated from the live dataset for it's table.
     */
    void received(IncomingStream stream);

    /**
     * This is called when we've received stream data we can't add to the received set for some reason,
     * usually when we've received data for a session which has been closed. The data backing this stream
     * should be deleted, and any resources associated with the given stream should be released.
     */
    void discardStream(IncomingStream stream);

    /**
     * Called when something went wrong with a stream session. All data associated with this receiver
     * should be deleted, and any associated resources should be cleaned up
     */
    void abort();

    /**
     * Called when a stream session has succesfully completed. All stream data being held by this receiver
     * should be added to the live data sets for their respective tables before this method returns.
     */
    void finished();

    /**
     * Called after finished has returned and we've sent any messages to other nodes. Mainly for
     * signaling that mvs and cdc should cleanup.
     */
    void cleanup();
}
