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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;

// This is not really a bench, but share common utilties from the base class.
// Running the class output compression ratio and dictionary effectiveness of different configurations
public class ZstdDictionaryCompressorRatioBench extends ZstdDictionaryCompressorBenchBase
{
    private static class CompressionResult
    {
        final String configuration;
        final double compressionRatio;
        final double dictionaryEffectiveness;

        CompressionResult(String configuration, double compressionRatio, double dictionaryEffectiveness)
        {
            this.configuration = configuration;
            this.compressionRatio = compressionRatio;
            this.dictionaryEffectiveness = dictionaryEffectiveness;
        }
    }

    private CompressionResult measureCompressionRatio() throws IOException
    {
        // Compress with current compressor
        inputBuffer.rewind();
        compressedBuffer.clear();
        compressor.compress(inputBuffer, compressedBuffer);
        int compressedSize = compressedBuffer.position();

        // Compress without dictionary for comparison
        inputBuffer.rewind();
        compressedBuffer.clear();
        noDictCompressor.compress(inputBuffer, compressedBuffer);
        int noDictCompressedSize = compressedBuffer.position();

        // Calculate ratios
        double compressionRatio = (double) inputBuffer.limit() / compressedSize;
        double dictionaryEffectiveness = (double) noDictCompressedSize / compressedSize;

        // Create configuration string
        String config = String.format("%s_%s_L%d_Chunk%dKiB",
                                      dataType,
                                      dictionarySize == 0 ? "NoDict" : "WithDict",
                                      compressionLevel,
                                      dataSize / 1024);

        return new CompressionResult(config, compressionRatio, dictionaryEffectiveness);
    }

    public static void main(String[] args) throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        List<CompressionResult> allResults = new ArrayList<>();

        // Define test parameters
        DataType[] dataTypes = {DataType.CASSANDRA_LIKE, DataType.COMPRESSIBLE, DataType.MIXED};
        int[] dictionarySizes = {0, 65536};
        int[] compressionLevels = {3, 5, 7};
        int[] dataSizes = {4096, 16384, 65536};

        System.out.println("Running ZSTD Dictionary Compressor Ratio Measurements...");
        System.out.println("Total configurations: " + (dataTypes.length * dictionarySizes.length * compressionLevels.length * dataSizes.length));

        int configCount = 0;
        for (DataType dataType : dataTypes)
        {
            for (int dictionarySize : dictionarySizes)
            {
                for (int compressionLevel : compressionLevels)
                {
                    for (int dataSize : dataSizes)
                    {
                        configCount++;
                        ZstdDictionaryCompressorRatioBench bench = new ZstdDictionaryCompressorRatioBench();
                        bench.dataType = dataType;
                        bench.dictionarySize = dictionarySize;
                        bench.compressionLevel = compressionLevel;
                        bench.dataSize = dataSize;

                        try
                        {
                            bench.setupIteration();
                            CompressionResult result = bench.measureCompressionRatio();
                            allResults.add(result);
                            bench.tearDown();
                        }
                        catch (Exception e)
                        {
                            System.err.println("Failed to process configuration: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        // Print consolidated results
        printConsolidatedResults(allResults);
    }

    private static void printConsolidatedResults(List<CompressionResult> results)
    {
        StringBuilder report = new StringBuilder();
        report.append("\n").append("=".repeat(100)).append("\n");
        report.append("ZSTD DICTIONARY COMPRESSOR RATIO RESULTS").append("\n");
        report.append("=".repeat(100)).append("\n");
        report.append(String.format("%-50s %-20s %-20s%n", "Configuration", "Compression Ratio", "Dictionary Effectiveness"));
        report.append("-".repeat(100)).append("\n");

        for (CompressionResult entry : results)
        {
            report.append(String.format("%-50s %-20.3f %-20.3f%n",
                entry.configuration, entry.compressionRatio, entry.dictionaryEffectiveness));
        }

        report.append("=".repeat(100)).append("\n");
        report.append("Compression Ratio: Original Size / Compressed Size (higher is better)").append("\n");
        report.append("Dictionary Effectiveness: Non-Dict Size / Dict Size (higher is better)").append("\n");
        report.append("=".repeat(100)).append("\n");

        System.out.print(report.toString());
    }
}
