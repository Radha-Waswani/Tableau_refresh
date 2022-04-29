create or replace view POWERDB.PLOG.STREAM_DDL_CREATE_VW(
	VIEW_DDL_FINAL
) as
select REPLACE(listagg(VIEW_DDL_MOD, ' '), ' UNION ', CHR(10)||' UNION '||CHR(10)) as VIEW_DDL_FINAL
from (
select concat(iff(RN=first_value(RN) over (order by RN),'create or replace view POWERDB.PLOG.STREAM_DDL_STORE_VW as ',''),
VIEW_DDL,iff(RN=last_value(RN) over (order by RN),'',' UNION ')) VIEW_DDL_MOD
from (
select *,row_number() over(order by TABLE_NAMESS) RN
from (
select distinct split_part(TABLE_NAMES,',',ROW_NUMBER() over(partition by VIEW_NAME,PROJECT_ID order by value)) as TABLE_NAMESS,
concat('select ',concat('''',TABLE_NAMESS,'''') ,' as TABLE_NAMES,count(*) ROW_COUNT,METADATA$ACTION,',
split_part(DATE_COLUMN_FORMATE,',',ROW_NUMBER() over(partition by VIEW_NAME,PROJECT_ID order by value)),
' from POWERDB.PLOG.','STREAM_',split_part(TABLE_NAMESS,'.',-1),' group by METADATA$ACTION,',
split_part(DATE_COLUMN_FORMATE,',',ROW_NUMBER() over(partition by VIEW_NAME,PROJECT_ID order by value))) VIEW_DDL
from POWERDB.PLOG.TABLEAU_REFRESH_DETAILS a,lateral split_to_table(a.TABLE_NAMES,',') )
)
);