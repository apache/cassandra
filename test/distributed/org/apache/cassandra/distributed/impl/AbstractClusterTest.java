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
package org.apache.cassandra.distributed.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.AssumptionViolatedException;
import org.junit.Test;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster.AbstractBuilder;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.FailingRunnable;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;


public class AbstractClusterTest
{
    @Test
    public void allowVnodeWithMultipleTokens()
    {
        AbstractBuilder builder = builder();
        builder.withTokenCount(42);
        unroll(() -> builder.createWithoutStarting());
    }

    @Test
    public void allowVnodeWithSingleToken()
    {
        AbstractBuilder builder = builder();
        builder.withTokenCount(1);
        unroll(() -> builder.createWithoutStarting());
    }

    @Test
    public void disallowVnodeWithMultipleTokens()
    {
        AbstractBuilder builder = builder();

        builder.withoutVNodes();
        builder.withTokenCount(42);

        Assertions.assertThatThrownBy(() -> builder.createWithoutStarting())
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("vnode is not supported");
    }


    @Test
    public void disallowVnodeWithSingleToken()
    {
        AbstractBuilder builder = builder();

        builder.withoutVNodes();
        builder.withTokenCount(1);

        unroll(() -> builder.createWithoutStarting());
    }

    @Test
    public void withoutVNodes()
    {
        AbstractBuilder builder = builder();

        builder.withoutVNodes();
        //TODO casting is annoying... what can be done to be smarter?
        builder.withTokenSupplier((TokenSupplier) i -> Arrays.asList("a", "b", "c"));

        Assertions.assertThatThrownBy(() -> builder.createWithoutStarting())
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("vnode is not supported");
    }

    @Test
    public void vnodeButTokensDoNotMatch()
    {
        AbstractBuilder builder = builder();

        builder.withTokenCount(1);
        //TODO casting is annoying... what can be done to be smarter?
        builder.withTokenSupplier((TokenSupplier) i -> Arrays.asList("a", "b", "c"));

        Assertions.assertThatThrownBy(() -> builder.createWithoutStarting())
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("no-vnode is requested but not supported");
    }

    @Test
    public void noVnodeButTokensDoNotMatch()
    {
        AbstractBuilder builder = builder();

        builder.withTokenCount(42);
        //TODO casting is annoying... what can be done to be smarter?
        builder.withTokenSupplier((TokenSupplier) i -> Arrays.asList("a"));

        Assertions.assertThatThrownBy(() -> builder.createWithoutStarting())
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("vnode is requested but not supported");
    }

    @Test
    public void vnodeNotSupported()
    {
        ConfigUpdate config = ConfigUpdate.of("num_tokens", 1);
        AbstractCluster<?> cluster = cluster(4, config);
        config.check = true;
        Assertions.assertThatThrownBy(() -> cluster.createInstanceConfig(1))
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("vnode is not supported");
    }

    @Test
    public void noVnodeNotSupported()
    {
        ConfigUpdate config = ConfigUpdate.of("num_tokens", 4);
        AbstractCluster<?> cluster = cluster(1, config);
        config.check = true;
        Assertions.assertThatThrownBy(() -> cluster.createInstanceConfig(1))
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("no-vnode is requested but not supported");
    }

    @Test
    public void vnodeMismatch()
    {
        ConfigUpdate config = ConfigUpdate.of("num_tokens", 4);
        AbstractCluster<?> cluster = cluster(2, config);
        config.check = true;
        Assertions.assertThatThrownBy(() -> cluster.createInstanceConfig(1))
                  .isInstanceOf(AssumptionViolatedException.class)
                  .hasMessage("vnode is enabled and num_tokens is defined in test without GOSSIP or setting initial_token");
    }

    @Test
    public void vnodeMismatchDefinesTokens()
    {
        ConfigUpdate config = ConfigUpdate.of("num_tokens", 4, "initial_token", "some values");
        AbstractCluster<?> cluster = cluster(2, config);
        config.check = true;
        unroll(() -> cluster.createInstanceConfig(1));
    }

    private static void unroll(FailingRunnable r)
    {
        try
        {
            r.run();
        }
        catch (AssumptionViolatedException e)
        {
            AssertionError e2 = new AssertionError(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
        }
    }

    private static AbstractCluster<?> cluster(int tokenCount, Consumer<IInstanceConfig> fn)
    {
        try
        {
            return (AbstractCluster<?>) builder()
                                        .withTokenCount(tokenCount)
                                        .withConfig(fn)
                                        .createWithoutStarting();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static AbstractBuilder builder()
    {
        return new Builder().withNodes(1);
    }

    private static class Builder extends AbstractBuilder<IInvokableInstance, Cluster, Builder>
    {
        public Builder()
        {
            super(Cluster::new);
        }
    }

    private static class Cluster extends AbstractCluster<IInvokableInstance>
    {
        protected Cluster(AbstractBuilder builder)
        {
            super(builder);
        }

        @Override
        protected IInvokableInstance newInstanceWrapper(Versions.Version version, IInstanceConfig config)
        {
            IInvokableInstance inst = Mockito.mock(IInvokableInstance.class);
            Mockito.when(inst.config()).thenReturn(config);
            return inst;
        }
    }

    private static class ConfigUpdate implements Consumer<IInstanceConfig>
    {
        public boolean check = false;
        private final Map<String, Object> override;

        private ConfigUpdate(Map<String, Object> override)
        {
            this.override = override;
        }

        public static ConfigUpdate of(Object... args)
        {
            Map<String, Object> override = new HashMap<>();
            for (int i = 0; i < args.length; i = i + 2)
                override.put((String) args[i], args[i + 1]);
            return new ConfigUpdate(override);
        }

        @Override
        public void accept(IInstanceConfig config)
        {
            if (!check)
                return;
            for (Map.Entry<String, Object> e : override.entrySet())
                config.set(e.getKey(), e.getValue());
        }
    }
}