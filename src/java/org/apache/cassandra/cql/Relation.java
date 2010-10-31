package org.apache.cassandra.cql;

/**
 * Relations encapsulate the relationship between an entity and a value. For
 * example, KEY > 'start' or COLUMN = 'somecolumn'.
 * 
 * @author eevans
 *
 */
public class Relation
{
    public Entity entity = Entity.COLUMN;
    public RelationType type;
    public Term value;
    
    /**
     * Creates a new relation.
     * 
     * @param entity the kind of relation this is; what the value is compared to.
     * @param type the type of relation; how how this entity relates to the value.
     * @param value the value being compared to the entity.
     */
    public Relation(String entity, String type, Term value)
    {
        if (entity.toUpperCase().equals("KEY"))
            this.entity = Entity.KEY;
        
        this.type = RelationType.forString(type);
        this.value = value;
    }
    
    public boolean isKey()
    {
        return entity.equals(Entity.KEY);
    }
    
    public boolean isColumn()
    {
        return entity.equals(Entity.COLUMN);
    }
}

enum Entity
{
    KEY, COLUMN;
}

enum RelationType
{
    EQ, LT, LTE, GTE, GT;
    
    public static RelationType forString(String s)
    {
        if (s.equals("="))
            return EQ;
        else if (s.equals("<"))
            return LT;
        else if (s.equals("<="))
            return LTE;
        else if (s.equals(">="))
            return GTE;
        else if (s.equals(">"))
            return GT;
        
        return null;
    }
}