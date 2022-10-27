CREATE OR REPLACE FUNCTION test.averageState(state tuple<int,bigint>, val int)
    CALLED ON NULL INPUT
    RETURNS tuple
    LANGUAGE java
    AS $$
        if (val != null) {
            state.setInt(0, state.getInt(0)+1);
            state.setLong(1, state.getLong(1)+val.intValue());
        }
        return state;
    $$;

CREATE OR REPLACE FUNCTION test.averageFinal (state tuple<int,bigint>)
    CALLED ON NULL INPUT
    RETURNS double
    LANGUAGE java
    AS $$
        double r = 0;
        if (state.getInt(0) == 0) return null;
        r = state.getLong(1);
        r /= state.getInt(0);
        return Double.valueOf(r);
    $$;

CREATE OR REPLACE AGGREGATE test.average(int)
    SFUNC averageState
    STYPE tuple
    FINALFUNC averageFinal
    INITCOND (0, 0);

CREATE TABLE test.atable (
    pk int PRIMARY KEY,
    val int
);

INSERT INTO test.atable (pk, val) VALUES (1,1);
INSERT INTO test.atable (pk, val) VALUES (2,2);
INSERT INTO test.atable (pk, val) VALUES (3,3);
INSERT INTO test.atable (pk, val) VALUES (4,4);

SELECT test.average(val) FROM atable;
