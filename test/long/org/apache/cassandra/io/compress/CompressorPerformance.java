package org.apache.cassandra.io.compress;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class CompressorPerformance
{

    public static void testPerformances() throws IOException
    {
        for (ICompressor compressor: new ICompressor[] {
                SnappyCompressor.instance,  // warm up
                DeflateCompressor.instance,
                LZ4Compressor.create(Collections.emptyMap()),
                SnappyCompressor.instance
        })
        {
            for (BufferType in: BufferType.values())
            {
                if (compressor.supports(in))
                {
                    for (BufferType out: BufferType.values())
                    {
                        if (compressor.supports(out))
                        {
                            for (int i=0; i<10; ++i)
                                testPerformance(compressor, in, out);
                            System.out.println();
                        }
                    }
                }
            }
        }
    }

    static ByteBuffer dataSource;
    static int bufLen;

    private static void testPerformance(ICompressor compressor, BufferType in, BufferType out) throws IOException
    {
        int len = dataSource.capacity();
        int bufLen = compressor.initialCompressedBufferLength(len);
        ByteBuffer input = in.allocate(bufLen);
        ByteBuffer output = out.allocate(bufLen);

        int checksum = 0;
        int count = 100;

        long time = System.nanoTime();
        for (int i=0; i<count; ++i)
        {
            output.clear();
            compressor.compress(dataSource, output);
            // Make sure not optimized away.
            checksum += output.get(ThreadLocalRandom.current().nextInt(output.position()));
            dataSource.rewind();
        }
        long timec = System.nanoTime() - time;
        output.flip();
        input.put(output);
        input.flip();

        time = System.nanoTime();
        for (int i=0; i<count; ++i)
        {
            output.clear();
            compressor.uncompress(input, output);
            // Make sure not optimized away.
            checksum += output.get(ThreadLocalRandom.current().nextInt(output.position()));
            input.rewind();
        }
        long timed = System.nanoTime() - time;
        System.out.format("Compressor %s %s->%s compress %.3f ns/b %.3f mb/s uncompress %.3f ns/b %.3f mb/s.%s\n",
                          compressor.getClass().getSimpleName(),
                          in,
                          out,
                          1.0 * timec / (count * len),
                          Math.scalb(1.0e9, -20) * count * len / timec,
                          1.0 * timed / (count * len),
                          Math.scalb(1.0e9, -20) * count * len / timed,
                          checksum == 0 ? " " : "");
    }

    public static void main(String[] args) throws IOException
    {
        try (FileInputStream fis = new FileInputStream("CHANGES.txt"))
        {
            int len = (int)fis.getChannel().size();
            dataSource = ByteBuffer.allocateDirect(len);
            while (dataSource.hasRemaining()) {
                fis.getChannel().read(dataSource);
            }
            dataSource.flip();
        }
        testPerformances();
    }
}
