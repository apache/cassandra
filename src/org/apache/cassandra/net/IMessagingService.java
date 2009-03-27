/**
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

package org.apache.cassandra.net;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.cassandra.concurrent.IStage;


/**
 * An IMessagingService provides the methods for sending messages to remote
 * endpoints. IMessagingService enables the sending of request-response style
 * messages and fire-forget style messages.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IMessagingService
{   
	/**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     * @param type name of the verb.     
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(String type, IVerbHandler verbHandler);
    
    /**
     * Deregister all verbhandlers corresponding to localEndPoint.
     * @param localEndPoint
     */
    public void deregisterAllVerbHandlers(EndPoint localEndPoint);
    
    /**
     * Deregister a verbhandler corresponding to the verb from the
     * Messaging Service.
     * @param type name of the verb.      
     */
    public void deregisterVerbHandlers(String type);
    
    /**
     * Listen on the specified port.
     * @param ep EndPoint whose port to listen on.
     * @param isHttp specify if the port is an Http port.     
     */
    public void listen(EndPoint ep, boolean isHttp) throws IOException;
    
    /**
     * Listen on the specified port.
     * @param ep EndPoint whose port to listen on.     
     */
    public void listenUDP(EndPoint ep);
    
    /**
     * Send a message to a given endpoint. 
     * @param message message to be sent.
     * @param to endpoint to which the message needs to be sent
     * @return an reference to an IAsyncResult which can be queried for the
     * response
     */
    public IAsyncResult sendRR(Message message, EndPoint to);

    /**
     * Send a message to the given set of endpoints and informs the MessagingService
     * to wait for at least <code>howManyResults</code> responses to determine success
     * of failure.
     * @param message message to be sent.
     * @param to endpoints to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses
     * @return an reference to message id used to match with the result
     */
    public String sendRR(Message message, EndPoint[] to, IAsyncCallback cb);
    
    /**
     * Send a message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * @param message message to be sent.
     * @param to endpoint to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses or
     *           suggest that a timeout occured to the invoker of the send().
     *           suggest that a timeout occured to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public String sendRR(Message message, EndPoint to, IAsyncCallback cb);

    /**
     * Send a message to a given endpoint. The ith element in the <code>messages</code>
     * array is sent to the ith element in the <code>to</code> array.This method assumes
     * there is a one-one mapping between the <code>messages</code> array and
     * the <code>to</code> array. Otherwise an  IllegalArgumentException will be thrown.
     * This method also informs the MessagingService to wait for at least
     * <code>howManyResults</code> responses to determine success of failure.
     * @param messages messages to be sent.
     * @param to endpoints to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses or
     *           suggest that a timeout occured to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public String sendRR(Message[] messages, EndPoint[] to, IAsyncCallback cb);
    
    /**
     * Send a message to a given endpoint. The ith element in the <code>messages</code>
     * array is sent to the ith element in the <code>to</code> array.This method assumes
     * there is a one-one mapping between the <code>messages</code> array and
     * the <code>to</code> array. Otherwise an  IllegalArgumentException will be thrown.
     * This method also informs the MessagingService to wait for at least
     * <code>howManyResults</code> responses to determine success of failure.
     * @param messages messages to be sent.
     * @param to endpoints to which the message needs to be sent
     * @return an reference to IAsyncResult
     */
    public IAsyncResult sendRR(Message[] messages, EndPoint[] to);
    
    /**
     * Send a message to a given endpoint. The ith element in the <code>messages</code>
     * array is sent to the ith element in the <code>to</code> array.This method assumes
     * there is a one-one mapping between the <code>messages</code> array and
     * the <code>to</code> array. Otherwise an  IllegalArgumentException will be thrown.
     * The idea is that multi-groups of messages are grouped as one logical message
     * whose results are harnessed via the <i>IAsyncResult</i>
     * @param messages groups of grouped messages.
     * @param to destination for the groups of messages
     * @param the callback handler to be invoked for the responses
     * @return the group id which is basically useless - it is only returned for API's
     *         to look compatible.
     */
    public String sendRR(Message[][] messages, EndPoint[][] to, IAsyncCallback cb);

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    public void sendOneWay(Message message, EndPoint to);
        
    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    public void sendUdpOneWay(Message message, EndPoint to);
    
    /**
     * Stream a file from source to destination. This is highly optimized
     * to not hold any of the contents of the file in memory.
     * @param file name of file to stream.
     * param start position inside the file
     * param total number of bytes to stream
     * param to endpoint to which we need to stream the file.
    */
    public void stream(String file, long startPosition, long total, EndPoint from, EndPoint to);

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     * @param verb for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(String verb);    
}
