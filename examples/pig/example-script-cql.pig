-- CqlNativeStorage
libdata = LOAD 'cql://libdata/libout' USING CqlNativeStorage();
book_by_mail = FILTER libdata BY C_OUT_TY == 'BM';

libdata_buildings = FILTER libdata BY SQ_FEET > 0;
state_flat = FOREACH libdata_buildings GENERATE STABR AS State,SQ_FEET AS SquareFeet;
state_grouped = GROUP state_flat BY State;
state_footage = FOREACH state_grouped GENERATE group AS State, SUM(state_flat.SquareFeet) AS TotalFeet:int;

insert_format= FOREACH state_footage GENERATE TOTUPLE(TOTUPLE('year',2011),TOTUPLE('state',State)),TOTUPLE(TotalFeet);
STORE insert_format INTO 'cql://libdata/libsqft?output_query=UPDATE%20libdata.libsqft%20SET%20sqft%20%3D%20%3F' USING CqlNativeStorage;