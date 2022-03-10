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
package org.apache.cassandra.inject;

import java.util.UUID;

/**
 * Refer to <a href="https://github.com/bytemanproject/byteman/blob/master/docs/asciidoc/src/main/asciidoc/chapters/Byteman-Rule-Language.adoc"/>
 * and injections.md files in the root directory.
 */
public class Rule
{
    public final String id;
    public final String script;
    public final String classToPreload;

    public static Rule newRule(String script)
    {
        return newRule(UUID.randomUUID().toString(), script);
    }

    public static Rule newRule(ActionBuilder actionBuilder, InvokePointBuilder invokePointBuilder)
    {
        return newRule(UUID.randomUUID().toString(), actionBuilder, invokePointBuilder);
    }

    public static Rule newRule(String id, String script)
    {
        return new Rule(id, script, null);
    }

    public static Rule newRule(String id, ActionBuilder actionBuilder, InvokePointBuilder invokePointBuilder)
    {
        String script = String.format("RULE %s\n%s\n%s\nENDRULE", id, invokePointBuilder.buildInternal(), actionBuilder.buildInternal());
        return new Rule(id, script, invokePointBuilder.getTargetClassOrInterface());
    }

    private Rule(String id, String script, String classToPreload)
    {
        this.id = id;
        this.script = script;
        this.classToPreload = classToPreload;
    }
}
