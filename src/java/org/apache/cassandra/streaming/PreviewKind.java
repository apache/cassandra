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


import java.util.UUID;

public enum PreviewKind
{
    NONE(0),
    ALL(1),
    UNREPAIRED(2),
    REPAIRED(3);

    private final int serializationVal;

    PreviewKind(int serializationVal)
    {
        assert ordinal() == serializationVal;
        this.serializationVal = serializationVal;
    }

    public int getSerializationVal()
    {
        return serializationVal;
    }

    public static PreviewKind deserialize(int serializationVal)
    {
        return values()[serializationVal];
    }


    public boolean isPreview()
    {
        return this != NONE;
    }

    public String logPrefix()
    {
        return isPreview() ? "preview repair" : "repair";
    }

    public String logPrefix(UUID sessionId)
    {
        return '[' + logPrefix() + " #" + sessionId.toString() + ']';
    }

}
