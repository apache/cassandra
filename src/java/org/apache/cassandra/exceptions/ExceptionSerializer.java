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

package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NullableSerializer;

import static java.util.Collections.newSetFromMap;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

/**
 * Support for serializing exceptions without a dependency on being able to instantiate the exception class
 * on the other side to eliminate dependencies across versions.
 *
 * This is still slightly more flexible than sending a string representation of the exception because it's still an exception so using it
 * as a cause or suppressed exception works and it is formatted nicely as if it were another local exception.
 */
public class ExceptionSerializer
{
    public static class RemoteException extends RuntimeException
    {
        private final String originalClass;

        public RemoteException(String originalClass, String originalMessage, StackTraceElement[] stackTrace)
        {
            super(originalMessage);
            this.originalClass = originalClass;
            setStackTrace(stackTrace);
        }

        private void initSuppressedAndCause(RemoteException cause, RemoteException[] suppressed)
        {
            initCause(cause);
            for (RemoteException e : suppressed)
                addSuppressed(e);
        }

        @Override
        public String toString()
        {
            String message = getMessage();
            return message != null ? originalClass + ": " + message : originalClass;
        }
    }

    static String getMessageWithOriginatingHost(Throwable t, boolean isFirstException)
    {
        if (isFirstException)
            return "Remote exception from host " + FBUtilities.getBroadcastAddressAndPort().toString() + (t.getLocalizedMessage() != null ? " - " + t.getLocalizedMessage() : "");
        else
            return t.getLocalizedMessage();
    }

    private static final IVersionedSerializer<StackTraceElement> stackTraceElementSerializer = new IVersionedSerializer<StackTraceElement>()
    {
        @Override
        public void serialize(StackTraceElement t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(t.getClassName());
            out.writeUTF(t.getMethodName());
            out.writeBoolean(t.getFileName() != null);
            if (t.getFileName() != null)
                out.writeUTF(t.getFileName());
            out.writeUnsignedVInt32(t.getLineNumber());
        }

        @Override
        public StackTraceElement deserialize(DataInputPlus in, int version) throws IOException
        {
            String className = in.readUTF();
            String methodName = in.readUTF();
            String fileName = null;
            if (in.readBoolean())
                fileName = in.readUTF();
            int lineNumber = in.readUnsignedVInt32();
            return new StackTraceElement(className, methodName, fileName, lineNumber);
        }

        @Override
        public long serializedSize(StackTraceElement t, int version)
        {
            long size = sizeof(t.getClassName()) +
                        sizeof(t.getMethodName()) +
                        sizeof(t.getFileName() != null) +
                        sizeofUnsignedVInt(t.getLineNumber());
            if (t.getFileName() != null)
                size += sizeof(t.getFileName());
            return size;
        }
    };

    public static final IVersionedSerializer<Throwable> remoteExceptionSerializer = new IVersionedSerializer<Throwable>()
    {
        @Override
        public void serialize(Throwable t, DataOutputPlus out, int version) throws IOException
        {
            Map<Throwable, Integer> alreadySerialized = new IdentityHashMap<>();
            serializeNextException(t, out, true, version, 0, alreadySerialized);
        }

        private int serializeNextException(Throwable t, DataOutputPlus out, boolean isFirstException, int version, int nextExceptionId, Map<Throwable, Integer> alreadySerialized) throws IOException
        {
            if (alreadySerialized.containsKey(t))
            {
                out.writeInt(alreadySerialized.get(t));
                return nextExceptionId;
            }
            else
            {
                alreadySerialized.put(t, nextExceptionId);
                out.writeInt(nextExceptionId);
                nextExceptionId++;
            }

            out.writeUTF(t.getClass().getName());
            String message = getMessageWithOriginatingHost(t, isFirstException);
            out.writeBoolean(message != null);
            if (message != null)
                out.writeUTF(message);
            ArraySerializers.serializeArray(t.getStackTrace(), out, version, stackTraceElementSerializer);

            // Do cause and suppressed last so they can reference back to previously partially deserialized exceptions
            out.writeBoolean(t.getCause() != null);
            if (t.getCause() != null)
                nextExceptionId = serializeNextException(t.getCause(), out, false, version, nextExceptionId, alreadySerialized);
            out.writeUnsignedVInt32(t.getSuppressed().length);
            for (Throwable suppressed : t.getSuppressed())
                nextExceptionId = serializeNextException(suppressed, out, false, version, nextExceptionId, alreadySerialized);

            return nextExceptionId;
        }

        @Override
        public Throwable deserialize(DataInputPlus in, int version) throws IOException
        {
            Map<Integer, Throwable> alreadyDeserialized = new HashMap<>();
            return deserializeNextException(in, version, alreadyDeserialized);
        }

        private Throwable deserializeNextException(DataInputPlus in, int version, Map<Integer, Throwable> alreadyDeserialized) throws IOException
        {
            int nextExceptionId = in.readInt();
            Throwable alreadyDeserializedThrowable = alreadyDeserialized.get(nextExceptionId);
            if (alreadyDeserializedThrowable != null)
                return alreadyDeserializedThrowable;

            String originalClass = in.readUTF();
            String originalMessage = null;
            if (in.readBoolean())
                originalMessage = in.readUTF();

            StackTraceElement[] stackTrace = ArraySerializers.deserializeArray(in, version, stackTraceElementSerializer, size -> new StackTraceElement[size]);
            RemoteException deserializedException = new RemoteException(originalClass, originalMessage, stackTrace);
            deserializedException.setStackTrace(stackTrace);
            alreadyDeserialized.put(nextExceptionId, deserializedException);

            // Do cause and suppressed last after alreadyDeserialized contains the exception we just processsed
            RemoteException cause = in.readBoolean() ? (RemoteException)deserializeNextException(in, version, alreadyDeserialized) : null;
            RemoteException[]  suppressed = new RemoteException[in.readUnsignedVInt32()];
            for (int i = 0; i < suppressed.length; i++)
                suppressed[i] = (RemoteException)deserializeNextException(in, version, alreadyDeserialized);
            deserializedException.initSuppressedAndCause(cause, suppressed);

            return deserializedException;
        }

        @Override
        public long serializedSize(Throwable t, int version)
        {
            Set<Throwable> alreadySeen = newSetFromMap(new IdentityHashMap<>());
            return nextExceptionSerializedSize(t, version, true, alreadySeen);
        }

        private long nextExceptionSerializedSize(Throwable t, int version, boolean isFirstException, Set<Throwable> alreadySeen)
        {
            if (!alreadySeen.add(t))
                return sizeof(42); // Exception ID from the last time it was serialized

            String message = getMessageWithOriginatingHost(t, isFirstException);
            long size = sizeof(42) + // Exception ID generated during serialization
                        sizeof(t.getClass().getName()) +
                        sizeof(message != null) +
                        (message != null ? sizeof(message) : 0) +
                        sizeof(t.getCause() != null) +
                        (t.getCause() != null ? nextExceptionSerializedSize(t.getCause(), version, false, alreadySeen) : 0) +
                        sizeofUnsignedVInt(t.getSuppressed().length);
            size += ArraySerializers.serializedArraySize(t.getStackTrace(), version, stackTraceElementSerializer);
            for (Throwable suppressed : t.getSuppressed())
                size += nextExceptionSerializedSize(suppressed, version, false, alreadySeen);
            return size;
        }
    };

    public static final IVersionedSerializer<Throwable> nullableRemoteExceptionSerializer = NullableSerializer.wrap(remoteExceptionSerializer);
}
