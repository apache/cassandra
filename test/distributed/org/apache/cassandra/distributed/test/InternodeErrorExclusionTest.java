package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.InboundConnectionInitiator;
import org.apache.cassandra.transport.SimpleClient;

import static org.assertj.core.api.Assertions.assertThat;

public class InternodeErrorExclusionTest extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClass2()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void ignoreAuthErrors() throws IOException, TimeoutException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c
                                                       .with(Feature.NETWORK)
                                                       .set("internode_authenticator", AlwaysFailingIInternodeAuthenticator.class.getName())
                                                       .set("internode_error_reporting_exclusions", ImmutableMap.of("subnets", Arrays.asList("127.0.0.1"))))
                                      .start())
        {
            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 7012).build())
            {
                client.connect(true);
                Assert.fail("Connection should fail");
            }
            catch (Exception e)
            {
                // expected
            }
            assertThat(cluster.get(1).logs().watchFor("address contained in internode_error_reporting_exclusions").getResult()).hasSize(1);
        }
    }

    public static class AlwaysFailingIInternodeAuthenticator implements IInternodeAuthenticator
    {
        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort)
        {
            String klass = InboundConnectionInitiator.class.getName();
            for (StackTraceElement e : Thread.currentThread().getStackTrace())
            {
                if (e.getClassName().startsWith(klass))
                    return false;
            }
            return true;
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {

        }
    }
}
