package org.apache.cassandra.cli;

public class CliUtils
{
    /*
     * Strips leading and trailing "'" characters, and handles
     * and escaped characters such as \n, \r, etc.
     * [Shameless clone from hive.]
     */
    public static String unescapeSQLString(String b) 
    {
        assert(b.charAt(0) == '\'');
        assert(b.charAt(b.length()-1) == '\'');
        StringBuilder sb = new StringBuilder(b.length());
        
        for (int i=1; i+1<b.length(); i++)
        {
            if (b.charAt(i) == '\\' && i+2<b.length())
            {
                char n=b.charAt(i+1);
                switch(n)
                {
                case '0': sb.append("\0"); break;
                case '\'': sb.append("'"); break;
                case '"': sb.append("\""); break;
                case 'b': sb.append("\b"); break;
                case 'n': sb.append("\n"); break;
                case 'r': sb.append("\r"); break;
                case 't': sb.append("\t"); break;
                case 'Z': sb.append("\u001A"); break;
                case '\\': sb.append("\\"); break;
                case '%': sb.append("%"); break;
                case '_': sb.append("_"); break;
                default: sb.append(n);
                }
            } 
            else
            {
                sb.append(b.charAt(i));
            }
        }
        return sb.toString();
    } 
}
