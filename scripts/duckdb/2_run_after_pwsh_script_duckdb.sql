
-- Notes:
--     * Suggests use Uniqueid in the PowerShell script obtaining time series


duckdb


-- Read all CSV files in the <out> folder
create or replace table tmp as
    select
        * replace(
            parse_filename(filename, true, 'system') as filename
        )
    from read_csv(
        'out/**/*.csv',
        skip=11,
        types={'TimeStamp': 'TIMESTAMP'},
        union_by_name=true,
        filename=true
    )
;


-- Create a frame `tmp_long` (in long format)
create or replace table tmp_long as
    with cte as (
        unpivot tmp
        on columns(* exclude(TimeStamp, filename))
        into
            name ID
            value Value
        order by ID, TimeStamp
    )
    select
        TimeStamp,
        Value,
        split_part(ID, '@', 2) as Location,
        split_part(ID, '@', 1) as Parameter,
        filename as uid
    from cte
;


-- Read the JSON files ('plate_info.json', 'plate_info.json') from <info> folder
create or replace table plate as
    with id_site as (
        select * as id_site from 'info/plate_info.json'
    )
    select
        unnest(map_keys(id_site)) as Location,
        unnest(map_values(id_site)) as Site
    from id_site
;
create or replace table param as
    with param_unit as (
        select * as param_unit from 'info/param_info.json'
    )
    select
        unnest(map_keys(param_unit)) as Parameter,
        unnest(map_values(param_unit)) as Unit
    from param_unit
;


-- Create a long format frame before save
create or replace table df_long as
    select t.TimeStamp, t.Value, pa.Unit, t.Parameter, pl.*, t.uid
    from tmp_long t
    left join plate pl on t.Location = pl.Location
    left join param pa on t.Parameter = pa.Parameter
    order by Site, TimeStamp
;
-- from df_long;


-- Export the frame (in long format)
copy (
    select * replace(strftime(TimeStamp, '%Y-%m-%d %H:%M:%S') as TimeStamp)
    from df_long
) to 'out/long_duckdb.parquet';

