package org.apache.cassandra.tools;

import com.github.rvesse.airline.io.printers.UsagePrinter;
import com.google.common.base.Splitter;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LegacyUsagePrinter extends UsagePrinter
{
    private final PrintWriter out;
    private final int maxSize;
    private final int indent;
    private final int hangingIndent;
    private final AtomicInteger currentPosition;

    public LegacyUsagePrinter(PrintWriter out, int maxSize)
    {
        this(out, maxSize, 0, 0, new AtomicInteger());
    }

    public LegacyUsagePrinter(PrintWriter out, int maxSize, int indent, int hangingIndent, AtomicInteger currentPosition)
    {
        super(out, maxSize, indent, hangingIndent, currentPosition);
        this.out = out;
        this.maxSize = maxSize;
        this.indent = indent;
        this.hangingIndent = hangingIndent;
        this.currentPosition = currentPosition;
    }

    public LegacyUsagePrinter newIndentedPrinter(int size)
    {
        return new LegacyUsagePrinter(out, maxSize, indent + size, hangingIndent, currentPosition);
    }

    public LegacyUsagePrinter newPrinterWithHangingIndent(int size)
    {
        return new LegacyUsagePrinter(out, maxSize, indent, hangingIndent + size, currentPosition);
    }

    public LegacyUsagePrinter newline()
    {
        out.append("\n");
        currentPosition.set(0);
        return this;
    }

    public LegacyUsagePrinter appendTable(Iterable<? extends Iterable<String>> table)
    {
        List<Integer> columnSizes = new ArrayList<>();
        for (Iterable<String> row : table) {
            int column = 0;
            for (String value : row) {
                while (column >= columnSizes.size()) {
                    columnSizes.add(0);
                }
                columnSizes.set(column, Math.max(value.length(), columnSizes.get(column)));
                column++;
            }
        }

        if (currentPosition.get() != 0) {
            currentPosition.set(0);
            out.append("\n");
        }

        for (Iterable<String> row : table) {
            int column = 0;
            StringBuilder line = new StringBuilder();
            for (String value : row) {
                int columnSize = columnSizes.get(column);
                line.append(value);
                line.append(spaces(columnSize - value.length()));
                line.append("   ");
                column++;
            }
            out.append(spaces(indent)).append(line.toString().trim()).append("\n");
        }

        return this;
    }

    public LegacyUsagePrinter append(String value)
    {
        if (value == null) {
            return this;
        }
        return appendWords(Splitter.onPattern("\\s+").omitEmptyStrings().trimResults().split(String.valueOf(value)));
    }

    public LegacyUsagePrinter appendWords(Iterable<String> words)
    {
        for (String word : words) {
            if (currentPosition.get() == 0) {
                // beginning of line
                out.append(spaces(indent));
                currentPosition.getAndAdd((indent));
            }
            else if (word.length() > maxSize || currentPosition.get() + word.length() <= maxSize) {
                // between words
                out.append(" ");
                currentPosition.getAndIncrement();
            }
            else {
                // wrap line
                out.append("\n").append(spaces(indent)).append(spaces(hangingIndent));
                currentPosition.set(indent);
            }

            out.append(word);
            currentPosition.getAndAdd((word.length()));
        }
        return this;
    }

    public UsagePrinter appendWords(Iterable<String> words, boolean avoidNewlines)
    {
        return appendWords(words);
    }

    private static String spaces(int count)
    {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < count; i++) {
            result.append(" ");
        }
        return result.toString();
    }
}
