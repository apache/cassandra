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

package org.apache.cassandra.net.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.BootstrapInitiateMessage;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class StreamContextManager
{
    private static Logger logger_ = Logger.getLogger(StreamContextManager.class);
    
    public static enum StreamCompletionAction
    {
        DELETE,
        STREAM
    }
    
    public static class StreamContext implements Serializable
    {
        private static Logger logger_ = Logger.getLogger(StreamContextManager.StreamContext.class);
        private static ICompactSerializer<StreamContext> serializer_;
        
        static
        {
            serializer_ = new StreamContextSerializer();
        }
        
        public static ICompactSerializer<StreamContext> serializer()
        {
            return serializer_;
        }
                
        private String targetFile_;        
        private long expectedBytes_;                     
        
        public StreamContext(String targetFile, long expectedBytes)
        {
            targetFile_ = targetFile;
            expectedBytes_ = expectedBytes;         
        }                
                
        public String getTargetFile()
        {
            return targetFile_;
        }
        
        public void setTargetFile(String file)
        {
            targetFile_ = file;
        }
        
        public long getExpectedBytes()
        {
            return expectedBytes_;
        }
                
        public boolean equals(Object o)
        {
            if ( !(o instanceof StreamContext) )
                return false;
            
            StreamContext rhs = (StreamContext)o;
            return targetFile_.equals(rhs.targetFile_);
        }
        
        public int hashCode()
        {
            return toString().hashCode();
        }
        
        public String toString()
        {
            return targetFile_ + ":" + expectedBytes_;
        }
    }
    
    public static class StreamContextSerializer implements ICompactSerializer<StreamContext>
    {
        public void serialize(StreamContextManager.StreamContext sc, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(sc.targetFile_);
            dos.writeLong(sc.expectedBytes_);            
        }
        
        public StreamContextManager.StreamContext deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            long expectedBytes = dis.readLong();           
            return new StreamContext(targetFile, expectedBytes);
        }
    }
    
    public static class StreamStatus
    {
        private static ICompactSerializer<StreamStatus> serializer_;
        
        static 
        {
            serializer_ = new StreamStatusSerializer();
        }
        
        public static ICompactSerializer<StreamStatus> serializer()
        {
            return serializer_;
        }
            
        private String file_;               
        private long expectedBytes_;                
        private StreamCompletionAction action_;
                
        public StreamStatus(String file, long expectedBytes)
        {
            file_ = file;
            expectedBytes_ = expectedBytes;
            action_ = StreamContextManager.StreamCompletionAction.DELETE;
        }
        
        public String getFile()
        {
            return file_;
        }
        
        public long getExpectedBytes()
        {
            return expectedBytes_;
        }
        
        void setAction(StreamContextManager.StreamCompletionAction action)
        {
            action_ = action;
        }
        
        public StreamContextManager.StreamCompletionAction getAction()
        {
            return action_;
        }
    }
    
    public static class StreamStatusSerializer implements ICompactSerializer<StreamStatus>
    {
        public void serialize(StreamStatus streamStatus, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(streamStatus.getFile());
            dos.writeLong(streamStatus.getExpectedBytes());
            dos.writeInt(streamStatus.getAction().ordinal());
        }
        
        public StreamStatus deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            long expectedBytes = dis.readLong();
            StreamStatus streamStatus = new StreamStatus(targetFile, expectedBytes);
            
            int ordinal = dis.readInt();                        
            if ( ordinal == StreamCompletionAction.DELETE.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.DELETE);
            }
            else if ( ordinal == StreamCompletionAction.STREAM.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.STREAM);
            }
            
            return streamStatus;
        }
    }
    
    public static class StreamStatusMessage implements Serializable
    {
        private static ICompactSerializer<StreamStatusMessage> serializer_;
        
        static 
        {
            serializer_ = new StreamStatusMessageSerializer();
        }
        
        public static ICompactSerializer<StreamStatusMessage> serializer()
        {
            return serializer_;
        }
        
        public static Message makeStreamStatusMessage(StreamStatusMessage streamStatusMessage) throws IOException
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream( bos );
            StreamStatusMessage.serializer().serialize(streamStatusMessage, dos);
            return new Message(StorageService.getLocalStorageEndPoint(), "", StorageService.bootStrapTerminateVerbHandler_, new Object[]{bos.toByteArray()});
        }
        
        protected StreamContextManager.StreamStatus streamStatus_;
        
        public StreamStatusMessage(StreamContextManager.StreamStatus streamStatus)
        {
            streamStatus_ = streamStatus;
        }
        
        public StreamContextManager.StreamStatus getStreamStatus()
        {
            return streamStatus_;
        }
    }
    
    public static class StreamStatusMessageSerializer implements ICompactSerializer<StreamStatusMessage>
    {
        public void serialize(StreamStatusMessage streamStatusMessage, DataOutputStream dos) throws IOException
        {
            StreamStatus.serializer().serialize(streamStatusMessage.streamStatus_, dos);            
        }
        
        public StreamStatusMessage deserialize(DataInputStream dis) throws IOException
        {            
            StreamContextManager.StreamStatus streamStatus = StreamStatus.serializer().deserialize(dis);         
            return new StreamStatusMessage(streamStatus);
        }
    }
        
    /* Maintain a stream context per host that is the source of the stream */
    public static Map<String, List<StreamContext>> ctxBag_ = new Hashtable<String, List<StreamContext>>();  
    /* Maintain in this map the status of the streams that need to be sent back to the source */
    public static Map<String, List<StreamStatus>> streamStatusBag_ = new Hashtable<String, List<StreamStatus>>();
    /* Maintains a callback handler per endpoint to notify the app that a stream from a given endpoint has been handled */
    public static Map<String, IStreamComplete> streamNotificationHandlers_ = new HashMap<String, IStreamComplete>();
    
    public synchronized static StreamContext getStreamContext(String key)
    {        
        List<StreamContext> context = ctxBag_.get(key);
        if ( context == null )
            throw new IllegalStateException("Streaming context has not been set.");
        StreamContext streamContext = context.remove(0);        
        if ( context.isEmpty() )
            ctxBag_.remove(key);
        return streamContext;
    }
    
    public synchronized static StreamStatus getStreamStatus(String key)
    {
        List<StreamStatus> status = streamStatusBag_.get(key);
        if ( status == null )
            throw new IllegalStateException("Streaming status has not been set.");
        StreamStatus streamStatus = status.remove(0);        
        if ( status.isEmpty() )
            streamStatusBag_.remove(key);
        return streamStatus;
    }
    
    /*
     * This method helps determine if the StreamCompletionHandler needs
     * to be invoked for the data being streamed from a source. 
    */
    public synchronized static boolean isDone(String key)
    {
        return (ctxBag_.get(key) == null);
    }
    
    public synchronized static IStreamComplete getStreamCompletionHandler(String key)
    {
        return streamNotificationHandlers_.get(key);
    }
    
    public synchronized static void removeStreamCompletionHandler(String key)
    {
        streamNotificationHandlers_.remove(key);
    }
    
    public synchronized static void registerStreamCompletionHandler(String key, IStreamComplete streamComplete)
    {
        streamNotificationHandlers_.put(key, streamComplete);
    }
    
    public synchronized static void addStreamContext(String key, StreamContext streamContext, StreamStatus streamStatus)
    {
        /* Record the stream context */
        List<StreamContext> context = ctxBag_.get(key);        
        if ( context == null )
        {
            context = new ArrayList<StreamContext>();
            ctxBag_.put(key, context);
        }
        context.add(streamContext);
        
        /* Record the stream status for this stream context */
        List<StreamStatus> status = streamStatusBag_.get(key);
        if ( status == null )
        {
            status = new ArrayList<StreamStatus>();
            streamStatusBag_.put(key, status);
        }
        status.add( streamStatus );
    }        
}
