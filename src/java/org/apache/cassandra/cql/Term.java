package org.apache.cassandra.cql;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Represents a term processed from a CQL query statement.  Terms are things
 * like strings, numbers, UUIDs, etc.
 * 
 * @author eevans
 *
 */
public class Term
{
    private final String text;
    private final TermType type;
    
    /**
     * Create new Term instance from a string, and an integer that corresponds
     * with the token ID from CQLParser.
     * 
     * @param text the text representation of the term.
     * @param type the term's type as an integer token ID.
     */
    public Term(String text, int type)
    {
        this.text = text;
        this.type = TermType.forInt(type);
    }
    
    protected Term()
    {
        this.text = "";
        this.type = TermType.STRING;
    }

    /**
     * Get the text that was parsed to create this term.
     * 
     * @return the string term as parsed from a CQL statement.
     */
    public String getText()
    {
        return text;
    }
    
    /**
     * Get the typed value, serialized to a byte[].
     * 
     * @return
     */
    public byte[] getBytes()
    {
        switch (type)
        {
            case STRING:
                return text.getBytes();
            case LONG:
                return FBUtilities.toByteArray(Long.parseLong(text));
        }
        
        // FIXME: handle scenario that should never happen
        return null;
    }

    /**
     * Get the term's type.
     * 
     * @return the type
     */
    public TermType getType()
    {
        return type;
    }
    
}

enum TermType
{
    STRING, LONG;
    
    static TermType forInt(int type)
    {
        if (type == CqlParser.STRING_LITERAL)
            return STRING;
        else if (type == CqlParser.LONG)
            return LONG;
        
        // FIXME: handled scenario that should never occur.
        return null;
    }
}
