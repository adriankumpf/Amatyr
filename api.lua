local cjson = require "cjson"
local ngx = ngx
local string = string
local io = require "io"
local assert = assert
local mysql = require "resty.mysql"
local conf
local pairs = pairs
local print = print

module(...)

if not conf then
    local f = assert(io.open(ngx.var.document_root .. "/etc/config.json", "r"))
    local c = f:read("*all")
    f:close()

    conf = cjson.decode(c)
end

-- Translate front end column names to back end column names
local function column(key)
    return conf.db.columns[key]
end

local function dbreq(sql)
    local db = mysql:new()
    db:set_timeout(30000)
    local ok, err = db:connect(
        {
            host=conf.db.host,
            port=conf.db.port,
            database=conf.db.database,
            user=conf.db.user,
            password=conf.db.password,
            compact_arrays=false
        })
    if not ok then
        ngx.say(err)
    end
    local res, err = db:query(sql)
    if not res then
        ngx.log(ngx.ERR, 'Failed SQL query:' .. sql)
        res = {error=err}
    end
    db:set_keepalive(0,10)

    return cjson.encode(res)
end

-- Helper function to get a start argument and return SQL constrains
local function getDateConstrains(startarg, interval)
    local where = ''
    local andwhere = ''
    if startarg then
        local start
        local endpart = "1 YEAR"
        if string.upper(startarg) == 'TODAY' then
            start = "CURDATE()"
            endpart = "1 DAY"
        elseif string.lower(startarg) == 'yesterday' then
            start = "DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y-%m-%d')"
            endpart = '1 DAY'
        elseif string.upper(startarg) == '3DAY' then
            start = "DATE_SUB(NOW(), INTERVAL 3 DAY)"
            endpart = '3 DAY'
        elseif string.upper(startarg) == 'WEEK' then
            start = "DATE_SUB(CURDATE(), INTERVAL 1 WEEK)"
            endpart = '1 WEEK'
        elseif string.upper(startarg) == '7DAYS' then
            start = "DATE_SUB(CURDATE(), INTERVAL 1 WEEK)"
            endpart = '1 WEEK'
        elseif string.upper(startarg) == 'MONTH' then
            start = "DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
            endpart = "1 MONTH"
        elseif string.upper(startarg) == 'YEAR' then
            start = "DATE_FORMAT(NOW(), '%Y-01-01')"
            endpart = "1 YEAR"
        elseif string.upper(startarg) == 'ALL' then
            start = "DATE '2016'" -- Should be old enough :-)
            endpart = "42 YEAR"
        else
            start = "DATE '" .. startarg .. "'"
        end
        -- use interval if provided, if not use the default endpart
        if not interval then
            interval = endpart
        end

        local wherepart = [[
        (
            ]]..column('datetime')..[[ BETWEEN UNIX_TIMESTAMP(]]..start..[[)
            AND UNIX_TIMESTAMP(DATE_ADD(]]..start..[[, INTERVAL ]]..endpart..[[))
        )
        ]]
        where = 'WHERE ' .. wherepart
        andwhere = 'AND ' .. wherepart
    end
    return where, andwhere
end

--- Return weather data by hour, week, month, year, whatever..
function by_dateunit(match)
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])

    local unit = 'hour'
    local join = [[
    SELECT
    c.dt as unit,
    IF ( @prevUnit != DAY(c.dt),
         ( @dailyrain := 0) + ((@prevUnit := DAY(c.dt))*0),
         ( @dailyrain := @dailyrain + c.rain )
    ) as dayrain
    FROM (
        SELECT
            ]]..datetrunc(unit)..[[ as dt,
            SUM(rain) as rain
        FROM archive
        ]]..where..[[
        GROUP BY 1
    ) as c
    ORDER BY 1
    ]]

    if match[1] and match[1] == 'month' or ngx.req.get_uri_args()['start'] == 'month' then
        unit = 'day'
        join = [[
        SELECT
            c.dt as unit,
            c.rain AS dayrain
        FROM (
          SELECT
              ]]..datetrunc(unit)..[[ as dt,
              SUM(]]..column('rain')..[[) as rain
          FROM ]]..conf.db.table..[[
          ]]..where..[[
          GROUP BY 1
        ) as c
        GROUP BY 1
        ORDER BY 1
        ]]
    end

    -- get the date constraints
    local sql = [[
    SELECT
        @dailyrain := 0,
        @prevUnit:=0,
        ]]..datetrunc(unit)..[[ AS datetime,
        AVG(]]..column('outtemp')..[[) as outtemp,
        MIN(]]..column('outtemp')..[[) as tempmin,
        MAX(]]..column('outtemp')..[[) as tempmax,
        AVG(]]..column('dewpoint')..[[) as dewpoint,
        AVG(]]..column('dewpoint1')..[[) as dewpoint1,
        AVG(]]..column('dewpoint2')..[[) as dewpoint2,
        AVG(]]..column('dewpoint3')..[[) as dewpoint3,
        AVG(]]..column('dewpoint4')..[[) as dewpoint4,
        AVG(]]..column('extratemp1')..[[) as extratemp1,
        AVG(]]..column('extratemp2')..[[) as extratemp2,
        AVG(]]..column('extratemp3')..[[) as extratemp3,
        AVG(]]..column('extratemp4')..[[) as extratemp4,
        AVG(]]..column('rooftemp')..[[) as rooftemp,
        AVG(]]..column('extrahumid1')..[[) as extrahumid1,
        AVG(]]..column('extrahumid2')..[[) as extrahumid2,
        AVG(]]..column('extrahumid3')..[[) as extrahumid3,
        AVG(]]..column('extrahumid4')..[[) as extrahumid4,
        AVG(]]..column('rain')..[[) as rain,
        b.dayrain as dayrain,
        AVG(]]..column('windspeed')..[[) as windspeed,
        AVG(]]..column('winddir')..[[) as winddir,
        AVG(]]..column('barometer')..[[) as barometer,
        AVG(]]..column('outhumidity')..[[) as outhumidity,
        AVG(]]..column('intemp')..[[) as intemp,
        AVG(]]..column('inhumidity')..[[) as inhumidity,
        AVG(]]..column('heatindex')..[[) as heatindex,
        AVG(]]..column('windchill')..[[) as windchill
    FROM ]]..conf.db.table..[[ as a
    LEFT OUTER JOIN (
    ]]..join..[[
    ) AS b
    ON ]]..datetrunc(unit)..[[ = b.unit
    ]]..where..[[
    GROUP BY ]]..datetrunc(unit)..[[
    ORDER BY datetime
    ]]


    return dbreq(sql)
end

-- Convert timezone of ]]..column('datetime')..[[, truncates to dateunit
function datetrunc(dateunit)
    if dateunit == 'minute' then
        return [[DATE_FORMAT(FROM_UNIXTIME(]]..column('datetime')..[[),"%Y-%m-%d %H:%i:00")]]
    elseif dateunit == 'second' then
        return [[DATE_FORMAT(FROM_UNIXTIME(]]..column('datetime')..[[),"%Y-%m-%d %H:%i:00")]]
    elseif dateunit == 'year' then
        return [[DATE_FORMAT(FROM_UNIXTIME(]]..column('datetime')..[[),"%Y-01-01")]]
    elseif dateunit == 'hour' then
        return [[DATE_FORMAT(FROM_UNIXTIME(]]..column('datetime')..[[),"%Y-%m-%d %H:00:00")]]
    elseif dateunit == 'day' then
        return [[DATE_FORMAT(FROM_UNIXTIME(]]..column('datetime')..[[),"%Y-%m-%d 00:00:00")]]
    end
    return [[FROM_UNIXTIME(]]..column('datetime')..[[)]]
end

function day(match)
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])
    local sql = [[
    SELECT
        ]]..datetrunc('minute')..[[ AS datetime,
        ]]..column('barometer')..[[ AS barometer,
        ]]..column('intemp')..[[ AS intemp,
        ]]..column('outtemp')..[[ AS outtemp,
        ]]..column('inhumidity')..[[ AS inhumidity,
        ]]..column('outhumidity')..[[ AS outhumidity,
        ]]..column('windspeed')..[[ AS windspeed,
        ]]..column('winddir')..[[ AS winddir,
        ]]..column('windgust')..[[ AS windgust,
        ]]..column('windgustdir')..[[ AS windgustdir,
        ]]..column('rainrate')..[[ AS rainrate,
        ]]..column('dewpoint')..[[ AS dewpoint,
        ]]..column('windchill')..[[ AS windchill,
        ]]..column('heatindex')..[[ AS heatindex,
        ]]..column('extratemp1')..[[ AS extratemp1,
        ]]..column('extratemp2')..[[ AS extratemp2,
        ]]..column('extratemp3')..[[ AS extratemp3,
        ]]..column('extratemp4')..[[ AS extratemp4,
        ]]..column('extrahumid1')..[[ AS extrahumid1,
        ]]..column('extrahumid2')..[[ AS extrahumid2,
        ]]..column('extrahumid3')..[[ AS extrahumid3,
        ]]..column('extrahumid4')..[[ AS extrahumid4,
        ]]..column('rooftemp')..[[ AS rooftemp,
        ]]..column('indewpoint')..[[ AS indewpoint,
        ]]..column('dewpoint1')..[[ AS dewpoint1,
        ]]..column('dewpoint2')..[[ AS dewpoint2,
        ]]..column('dewpoint3')..[[ AS dewpoint3,
        ]]..column('dewpoint4')..[[ AS dewpoint4,
        ]]..column('forecast')..[[ AS forecast,
        SUM(rain) AS dayrain
    FROM ]]..conf.db.table..[[
    ]]..where..[[
    GROUP BY datetime
    ORDER BY datetime
    ]]

    return dbreq(sql)
end

function year(match)
    -- This function generates stats into a new table
    -- which is updated max once a day
    -- first it checks the latest record in the stats table
    -- and if latest date is older than today
    -- it will recreate the table
    local year = match[1]
    local where = [[
        WHERE dateTime BETWEEN UNIX_TIMESTAMP(MAKEDATE(]]..year..[[, 01))
        AND UNIX_TIMESTAMP(DATE_ADD(MAKEDATE(]]..year..[[, 01), INTERVAL 1 year))
    ]]

    local needsupdate = cjson.decode(dbreq[[
        SELECT
        MAX(dateTime) < (UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 24 HOUR))) AS needsupdate
        FROM days
    ]])
    if needsupdate == ngx.null or needsupdate[1] == nil or needsupdate.error ~= nil then
        needsupdate = true
    else
        if needsupdate[1]['needsupdate'] == 't' then
            needsupdate = true
        else
            needsupdate = false
        end
    end
    if needsupdate then
        -- Remove existing cache. This could be improved to only add missing data
        dbreq('DROP TABLE days')
        -- Create new cached table
        local gendays = dbreq([[
        CREATE TABLE days AS (
            SELECT
                UNIX_TIMESTAMP(]]..datetrunc('day')..[[) AS dateTime,
                ]]..datetrunc('day')..[[ AS dt,
                AVG(]]..column('outtemp')..[[) as outtemp,
                MIN(]]..column('outtemp')..[[) as tempmin,
                MAX(]]..column('outtemp')..[[) as tempmax,
                AVG(]]..column('rain')..[[) as rain,
                AVG(]]..column('windspeed')..[[) as windspeed,
                AVG(]]..column('winddir')..[[) as winddir,
                AVG(]]..column('barometer')..[[) as barometer,
                AVG(]]..column('outhumidity')..[[) as outhumidity,
                AVG(]]..column('intemp')..[[) as intemp,
                AVG(]]..column('inhumidity')..[[) as inhumidity,
                AVG(]]..column('heatindex')..[[) as heatindex,
                AVG(]]..column('windgust')..[[) AS windgust,
                AVG(]]..column('windgustdir')..[[) AS windgustdir,
                AVG(]]..column('dewpoint')..[[) AS dewpoint,
                AVG(]]..column('windchill')..[[) AS windchill,
                AVG(]]..column('extratemp1')..[[) AS extratemp1,
                AVG(]]..column('extratemp2')..[[) AS extratemp2,
                AVG(]]..column('extratemp3')..[[) AS extratemp3,
                AVG(]]..column('extratemp4')..[[) AS extratemp4,
                AVG(]]..column('extrahumid1')..[[) AS extrahumid1,
                AVG(]]..column('extrahumid2')..[[) AS extrahumid2,
                AVG(]]..column('extrahumid3')..[[) AS extrahumid3,
                AVG(]]..column('extrahumid4')..[[) AS extrahumid4,
                AVG(]]..column('rooftemp')..[[) AS rooftemp,
                AVG(]]..column('indewpoint')..[[) AS indewpoint,
                AVG(]]..column('dewpoint1')..[[) AS dewpoint1,
                AVG(]]..column('dewpoint2')..[[) AS dewpoint2,
                AVG(]]..column('dewpoint3')..[[) AS dewpoint3,
                AVG(]]..column('dewpoint4')..[[) AS dewpoint4,
                b.dayrain
            FROM ]]..conf.db.table..[[ AS a
            LEFT OUTER JOIN
            (
                SELECT
                    UNIX_TIMESTAMP(]]..datetrunc('day')..[[) AS hour,
                    SUM(rain) AS dayrain
                    FROM ]]..conf.db.table..[[
                    GROUP BY 1
                    ORDER BY 1
            ) AS b
            ON UNIX_TIMESTAMP(]]..datetrunc('day')..[[) = b.hour
            GROUP BY 1
            ORDER BY 1
        )
        ]])
    end
    local sql = [[
        SELECT
        dt AS datetime,
        ]]..column('intemp')..[[ AS intemp,
        ]]..column('outtemp')..[[ AS outtemp,
        ]]..column('inhumidity')..[[ AS inhumidity,
        ]]..column('outhumidity')..[[ AS outhumidity,
        ]]..column('windspeed')..[[ AS windspeed,
        ]]..column('winddir')..[[ AS winddir,
        ]]..column('windgust')..[[ AS windgust,
        ]]..column('windgustdir')..[[ AS windgustdir,
        ]]..column('dewpoint')..[[ AS dewpoint,
        ]]..column('windchill')..[[ AS windchill,
        ]]..column('heatindex')..[[ AS heatindex,
        ]]..column('extratemp1')..[[ AS extratemp1,
        ]]..column('extratemp2')..[[ AS extratemp2,
        ]]..column('extratemp3')..[[ AS extratemp3,
        ]]..column('extratemp4')..[[ AS extratemp4,
        ]]..column('extrahumid1')..[[ AS extrahumid1,
        ]]..column('extrahumid2')..[[ AS extrahumid2,
        ]]..column('extrahumid3')..[[ AS extrahumid3,
        ]]..column('extrahumid4')..[[ AS extrahumid4,
        ]]..column('rooftemp')..[[ AS rooftemp,
        ]]..column('indewpoint')..[[ AS indewpoint,
        ]]..column('dewpoint1')..[[ AS dewpoint1,
        ]]..column('dewpoint2')..[[ AS dewpoint2,
        ]]..column('dewpoint3')..[[ AS dewpoint3,
        ]]..column('dewpoint4')..[[ AS dewpoint4,
        dayrain
        FROM days
        ]]..where
    return dbreq(sql)
end

function windhist(match)
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])
    local sql = [[
        SELECT FLOOR(COUNT(*)) AS count,
        CASE WHEN ]]..column('windspeed')..[[<2.0 THEN NULL ELSE (ROUND(]]..column('winddir')..[[/10,0)*10) END as d,
        AVG(]]..column('windspeed')..[[)*1.94384449 AS avg
        FROM ]]..conf.db.table..[[
        ]]..where..[[
        GROUP BY d
        ORDER BY d
    ]]

    return dbreq(sql)
end

-- Function to return extremeties from database, min/maxes for different time intervals
function record(match)

    local key = match[1]
    local func = string.upper(match[2])
    local period = string.upper(ngx.req.get_uri_args()['start'])
    local where, andwhere = getDateConstrains(period)
    local sql

    if key == 'dayrain' and (func == 'MAX' or func == 'MIN') then
        -- Not valid with any other value than max
        sql = [[
        SELECT
        DISTINCT ]]..datetrunc('day')..[[ AS datetime,
        SUM(rain) AS dayrain
        FROM ]]..conf.db.table..[[
        ]]..where..[[
        GROUP BY datetime
        ORDER BY dayrain DESC
        LIMIT 1
        ]]
    elseif func == 'SUM' then
        -- The SUM part doesn't need the datetime of the record since the datetime is effectively over the whole scope
        sql = [[
            SELECT
            SUM(]]..column(key)..[[) AS ]]..key..[[
            FROM ]]..conf.db.table..[[
            ]]..where..[[
        ]]
    else
        sql = [[
        SELECT
            ]]..datetrunc('')..[[ AS datetime,
            TIMESTAMPDIFF(DAY, FROM_UNIXTIME(]]..column('datetime')..[[), NOW()) AS age,
            ]]..column(key)..[[ AS ]]..key..[[
        FROM ]]..conf.db.table..[[
        WHERE
        ]]..column(key)..[[ =
        (
            SELECT
                ]]..func..[[(]]..column(key)..[[)
            FROM ]]..conf.db.table..[[
            ]]..where..[[
            LIMIT 1
        )
        ]]..andwhere..[[
        LIMIT 1
        ]]
    end

    return dbreq(sql)
end

function max(match)
    local key = ngx.req.get_uri_args()['key']
    if not key then ngx.exit(403) end
    -- Make sure valid request, only accept plain lowercase ascii string for key name
    local keytest = ngx.re.match(key, '[a-z]+', 'oj')
    if not keytest then ngx.exit(403) end

    local sql = [[
        SELECT
            ]]..datetrunc('day')..[[ AS datetime,
            MAX(]]..key..[[) AS ]]..key..[[
            FROM ]]..conf.db.table..[[
            WHERE date_part('year', FROM_UNIXTIME(]]..column('datetime')..[[)) > 2016
            GROUP BY 1
	]]

    return dbreq(sql)
end

-- Latest record in db
function now(match)
     local sql = [[
     SELECT
     ]]..column('datetime')..[[ AS datetime,
     ]]..column('barometer')..[[ AS barometer,
     ]]..column('intemp')..[[ AS intemp,
     ]]..column('outtemp')..[[ AS outtemp,
     ]]..column('inhumidity')..[[ AS inhumidity,
     ]]..column('outhumidity')..[[ AS outhumidity,
     ]]..column('windspeed')..[[ AS windspeed,
     ]]..column('winddir')..[[ AS winddir,
     ]]..column('windgust')..[[ AS windgust,
     ]]..column('windgustdir')..[[ AS windgustdir,
     ]]..column('rainrate')..[[ AS rainrate,
     ]]..column('dewpoint')..[[ AS dewpoint,
     ]]..column('windchill')..[[ AS windchill,
     ]]..column('heatindex')..[[ AS heatindex,
     ]]..column('extratemp1')..[[ AS extratemp1,
     ]]..column('extratemp2')..[[ AS extratemp2,
     ]]..column('extratemp3')..[[ AS extratemp3,
     ]]..column('extratemp4')..[[ AS extratemp4,
     ]]..column('extrahumid1')..[[ AS extrahumid1,
     ]]..column('extrahumid2')..[[ AS extrahumid2,
     ]]..column('extrahumid3')..[[ AS extrahumid3,
     ]]..column('extrahumid4')..[[ AS extrahumid4,
     ]]..column('rooftemp')..[[ AS rooftemp,
     ]]..column('indewpoint')..[[ AS indewpoint,
     ]]..column('dewpoint1')..[[ AS dewpoint1,
     ]]..column('dewpoint2')..[[ AS dewpoint2,
     ]]..column('dewpoint3')..[[ AS dewpoint3,
     ]]..column('dewpoint4')..[[ AS dewpoint4,
     ]]..column('forecast')..[[ AS forecast,
     (
         SELECT SUM(rain)
         FROM ]]..conf.db.table..[[
         WHERE ]]..column('datetime')..[[ >= UNIX_TIMESTAMP(CURRENT_DATE())
     ) AS dayrain
     FROM ]]..conf.db.table..[[
     ORDER BY datetime DESC LIMIT 1
     ]]

    return dbreq(sql);
end

-- Last 60 samples from db
function recent(match)
    return dbreq([[
        SELECT
        ]]..datetrunc('minute')..[[ AS datetime,
        ]]..column('barometer')..[[ AS barometer,
        ]]..column('intemp')..[[ AS intemp,
        ]]..column('outtemp')..[[ AS outtemp,
        ]]..column('inhumidity')..[[ AS inhumidity,
        ]]..column('outhumidity')..[[ AS outhumidity,
        ]]..column('windspeed')..[[ AS windspeed,
        ]]..column('winddir')..[[ AS winddir,
        ]]..column('windgust')..[[ AS windgust,
        ]]..column('windgustdir')..[[ AS windgustdir,
        ]]..column('rainrate')..[[ AS rainrate,
        ]]..column('dewpoint')..[[ AS dewpoint,
        ]]..column('windchill')..[[ AS windchill,
        ]]..column('heatindex')..[[ AS heatindex,
        ]]..column('extratemp1')..[[ AS extratemp1,
        ]]..column('extratemp2')..[[ AS extratemp2,
        ]]..column('extratemp3')..[[ AS extratemp3,
        ]]..column('extratemp4')..[[ AS extratemp4,
        ]]..column('extrahumid1')..[[ AS extrahumid1,
        ]]..column('extrahumid2')..[[ AS extrahumid2,
        ]]..column('extrahumid3')..[[ AS extrahumid3,
        ]]..column('extrahumid4')..[[ AS extrahumid4,
        ]]..column('rooftemp')..[[ AS rooftemp,
        ]]..column('indewpoint')..[[ AS indewpoint,
        ]]..column('dewpoint1')..[[ AS dewpoint1,
        ]]..column('dewpoint2')..[[ AS dewpoint2,
        ]]..column('dewpoint3')..[[ AS dewpoint3,
        ]]..column('dewpoint4')..[[ AS dewpoint4,
        ]]..column('forecast')..[[ AS forecast
        FROM ]] .. conf.db.table .. [[
        WHERE ]]..column('datetime')..[[ >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 HOUR))
        GROUP BY datetime
        ORDER BY datetime DESC;
    ]])
end
