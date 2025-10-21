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

package org.apache.cassandra.journal;

import java.util.function.Consumer;

/**
 * Helper file to avoid exposing components outside their package-local visibility scope
 */
public class DumpUtil
{
    public static void dumpMetadata(Descriptor descriptor, Consumer<String> out)
    {
        if (Component.METADATA.existsFor(descriptor))
        {
            out.accept(Metadata.load(descriptor).toString());
        }
        else
            out.accept("Metadata absent for " + descriptor);
    }

    public static <K, V> StaticSegment<K, V> open(Descriptor descriptor, KeySupport<K> keySupport)
    {
        return StaticSegment.open(descriptor, keySupport);
    }
}
