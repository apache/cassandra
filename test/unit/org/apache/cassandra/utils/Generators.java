package org.apache.cassandra.utils;

import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

public final class Generators
{
    private static final char[] REGEX_WORD_DOMAIN = createRegexWordDomain();

    private Generators()
    {

    }

    public static Gen<String> regexWord(Gen<Integer> sizes)
    {
        return string(sizes, REGEX_WORD_DOMAIN);
    }

    public static Gen<String> string(Gen<Integer> sizes, char[] domain)
    {
        // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
        return charArray(sizes, domain).map(c -> new String(c));
    }

    public static Gen<char[]> charArray(Gen<Integer> sizes, char[] domain)
    {
        Constraint constraints = Constraint.between(0, domain.length - 1).withNoShrinkPoint();
        Gen<char[]> gen = td -> {
            int size = sizes.generate(td);
            char[] is = new char[size];
            for (int i = 0; i != size; i++) {
                int idx = (int) td.next(constraints);
                is[i] = domain[idx];
            }
            return is;
        };
        gen.describedAs(String::new);
        return gen;
    }

    private static char[] createRegexWordDomain()
    {
        // \w == [a-zA-Z_0-9]
        char[] domain = new char[26 * 2 + 10 + 1];

        int offset = 0;
        // _
        domain[offset++] = (char) 95;
        // 0-9
        for (int c = 48; c < 58; c++)
            domain[offset++] = (char) c;
        // A-Z
        for (int c = 65; c < 91; c++)
            domain[offset++] = (char) c;
        // a-z
        for (int c = 97; c < 123; c++)
            domain[offset++] = (char) c;
        return domain;
    }
}
