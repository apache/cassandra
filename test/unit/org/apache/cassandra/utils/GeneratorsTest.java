package org.apache.cassandra.utils;

import com.google.common.net.InternetDomainName;
import org.junit.Test;

import org.assertj.core.api.Assertions;

import static org.quicktheories.QuickTheory.qt;

public class GeneratorsTest
{
    @Test
    public void randomUUID()
    {
        qt().forAll(Generators.UUID_RANDOM_GEN).checkAssert(uuid -> {
            Assertions.assertThat(uuid.version())
                      .as("version was not random uuid")
                      .isEqualTo(4);
            Assertions.assertThat(uuid.variant())
                      .as("varient not set to IETF (2)")
                      .isEqualTo(2);
        });
    }

    @Test
    public void dnsDomainName()
    {
        qt().forAll(Generators.DNS_DOMAIN_NAME).checkAssert(InternetDomainName::from);
    }
}