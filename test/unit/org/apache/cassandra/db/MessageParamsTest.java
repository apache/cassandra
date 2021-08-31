package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.net.ParamType;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageParamsTest
{
    @Test
    public void addIfLarger()
    {
        ParamType key = ParamType.TOMBSTONE_WARNING;

        // first time, so should update
        MessageParams.addIfLarger(key, 10);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(10);

        // this is smaller, so should ignore
        MessageParams.addIfLarger(key, 1);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(10);

        // should update as it is larger
        MessageParams.addIfLarger(key, 100);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(100);

        // not protecting against mixing types
        Assertions.assertThatThrownBy(() -> MessageParams.addIfLarger(key, "should fail"))
                  .isInstanceOf(ClassCastException.class);
    }
}