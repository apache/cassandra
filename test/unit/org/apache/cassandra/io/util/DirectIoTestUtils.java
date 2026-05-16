/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.util;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Throwables.ThrowingRunnable;

public final class DirectIoTestUtils
{
    public interface ThrowingSupplier<T>
    {
        T get() throws Exception;
    }

    private DirectIoTestUtils() {}

    public static void withDirectWrites(ThrowingRunnable body) throws Exception
    {
        withDirectWrites(() -> { body.run(); return null; });
    }

    public static <T> T withDirectWrites(ThrowingSupplier<T> body) throws Exception
    {
        DiskAccessMode original = DatabaseDescriptor.getBackgroundWriteDiskAccessMode();
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(DiskAccessMode.direct);
        try
        {
            return body.get();
        }
        finally
        {
            DatabaseDescriptor.setBackgroundWriteDiskAccessMode(original);
        }
    }
}
