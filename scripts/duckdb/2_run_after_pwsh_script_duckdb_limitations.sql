
/*
Notes (limitations):
    * Suggests use [uid] in the PowerShell script obtaining time series
    * The 2nd column in each CSV file is following [Parameter]@[Plate]. Therefore,
    NO way to request data on the same uid for different temporal resolutions.
    i.e., the same [Parameter] and the same [Plate] but different temporal resolutions
    will NOT work! Works when the [Parameter]s are different!
*/


-- duckdb


-- Create a long format frame before save
create or replace table df_long as
    -- Read all CSV files in the <out> folder, recursively -> `tmp`
    with tmp as (
        select
            * replace (parse_filename(filename, true, 'system') as filename)
        from read_csv(
            'out/**/*.csv',
            skip = 11,
            types = {'TimeStamp': 'TIMESTAMP'},
            union_by_name = true,
            filename = true
        )
    ),
    -- Create a temporary frame (in long format)
    cte as (
        unpivot tmp
        on columns(* exclude (TimeStamp, filename))
        into
            name ID
            value Value
        order by ID, TimeStamp
    ),
    -- Split column [ID] into columns [Location] and [Parameter]
    tmp_long as (
        select
            TimeStamp,
            Value,
            split_part(ID, '@', -1) as Location,
            split_part(ID, '@', 1) as Parameter,
            filename as uid
        from cte
    ),
    -- Read the JSON files ('plate_info.json') from <info> folder
    plates as (
        select
            unnest(map_keys(id_site)) as Location,
            unnest(map_values(id_site)) as Site
        from (
            select * as id_site
            from 'info/plate_info.json'
        )
    ),
    -- Read the JSON files ('plate_info.json') from <info> folder
    params as (
        select
            unnest(map_keys(param_unit)) as Parameter,
            unnest(map_values(param_unit)) as Unit
        from (
            select * as param_unit
            from 'info/param_info.json'
        )
    ),
    -- Add the extra information - Unit, and Site names
    ts_long as (
        select t.TimeStamp, t.Value, pa.Unit, t.Parameter, pl.*, t.uid
        from tmp_long t
        left join plates pl on t.Location = pl.Location
        left join params pa on t.Parameter = pa.Parameter
        order by t.Parameter, Site, TimeStamp
    )
    -- CAST the [TimeStamp] from TIMESTAMP to VARCHAR (optional)
    select * replace (strftime(TimeStamp, '%Y-%m-%d %H:%M:%S') as TimeStamp)
    from ts_long
;


-- show tables;
-- from df_long;


copy df_long to 'out/df_long.parquet';
