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

package org.apache.cassandra.utils;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

public final class Architecture
{
    // Note that s390x & aarch64 architecture are not officially supported and adding it here is only done out of convenience
    // for those that want to run C* on this architecture at their own risk (see #11214 & #13326)
    private static final Set<String> UNALIGNED_ARCH = Collections.unmodifiableSet(Sets.newHashSet(
        "i386",
        "x86",
        "amd64",
        "x86_64",
        "s390x",
        "aarch64"
    ));

    public static final boolean IS_UNALIGNED = UNALIGNED_ARCH.contains(System.getProperty("os.arch"));

    private Architecture()
    {
    }
}
