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

import javax.annotation.Nullable;

import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;

/**
 * Exception thrown during repair
 */
public class RepairException extends Exception
{
    private final boolean shouldLogWarn;

    private RepairException(@Nullable RepairJobDesc desc, PreviewKind previewKind, String message, boolean shouldLogWarn)
    {
        this((desc == null ? "" : desc.toString(previewKind != null ? previewKind : PreviewKind.NONE)) + ' ' + message, shouldLogWarn);
    }

    private RepairException(String msg, boolean shouldLogWarn)
    {
        super(msg);
        this.shouldLogWarn = shouldLogWarn;
    }

    public static RepairException error(@Nullable RepairJobDesc desc, PreviewKind previewKind, String message)
    {
        return new RepairException(desc, previewKind, message, false);
    }

    public static RepairException warn(@Nullable RepairJobDesc desc, PreviewKind previewKind, String message)
    {
        return new RepairException(desc, previewKind, message, true);
    }

    public static RepairException warn(String message)
    {
        return new RepairException(message, true);
    }

    public static boolean shouldWarn(Throwable throwable)
    {
        return throwable instanceof RepairException && ((RepairException)throwable).shouldLogWarn;
    }
}
