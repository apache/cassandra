package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectSizesTest
{
    @Test
    public void heapByteBuffer()
    {
        byte[] bytes = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long empty = ObjectSizes.sizeOfEmptyHeapByteBuffer();
        long actual = ObjectSizes.measureDeep(buffer);

        assertThat(actual).isEqualTo(empty + ObjectSizes.sizeOfArray(bytes));
        assertThat(ObjectSizes.sizeOnHeapOf(buffer)).isEqualTo(actual);
    }
}