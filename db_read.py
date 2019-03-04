import pyodbc
import pandas as pd

#The language placeholder string
lang_placeholder = '%lang'

Q_FACTS = """\
select
d.Data_NId as data_id,
i.Indicator_Name as INDICATOR,
a.Area_Name as REF_AREA,
a.Area_ID as REF_AREA_CODE,
al.Area_Level_Name as REF_AREA_TYPE,
t.TimePeriod as TIME_PERIOD,
u.Unit_Name as UNIT_MEASURE,
d.Data_Value as OBS_VALUE,
IIF(f.FootNote='',NULL,f.FootNote) as FOOTNOTE,

d.Subgroup_Val_NId,
d.Indicator_NId

from ((((((UT_Data as d
INNER JOIN UT_FootNote_%lang as f on f.FootNote_NId = d.FootNote_NId)
INNER JOIN UT_TimePeriod as t on t.TimePeriod_NId = d.TimePeriod_NId)
INNER JOIN UT_Unit_%lang as u on u.Unit_NId = d.Unit_NId)
INNER JOIN UT_Area_%lang as a on a.Area_NId = d.Area_NId)
INNER JOIN UT_Area_Level_%lang al on a.Area_Level = al.Area_Level)
INNER JOIN UT_Indicator_%lang as i on i.Indicator_NId = d.Indicator_NId)
"""

Q_DIMS = """\
SELECT  sv.Subgroup_Val_NId, sgt.Subgroup_Type_Name, sg.Subgroup_Name
FROM (((UT_Subgroup_Vals_%lang as sv
INNER JOIN UT_Subgroup_Vals_Subgroup as svsg on svsg.Subgroup_Val_NId = sv.Subgroup_Val_NId)
INNER JOIN UT_Subgroup_%lang as sg on sg.Subgroup_NId = svsg.Subgroup_NId)
INNER JOIN UT_Subgroup_Type_%lang as sgt on sgt.Subgroup_Type_NId = sg.Subgroup_Type)
ORDER BY sv.Subgroup_Val_NId
"""

Q_SECTORS = """\
Select cat.id as Indicator_NId, cat.SECTOR as SECTOR FROM(
SELECT DISTINCT i.Indicator_NId as id, c.IC_Name as SECTOR from (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'SC' and c.IC_Parent_NId = -1) as cat
"""

Q_SUBSECTORS = """\
Select cat.id as Indicator_NId, cat.SECTOR as SUBSECTOR FROM(
SELECT DISTINCT i.Indicator_NId as id, c.IC_Name as SECTOR 
FROM (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'SC' and c.IC_Parent_NId <> -1) as cat
"""

Q_SOURCES = """\
Select d.Data_Nid as data_id, s.IC_NAME as SOURCE FROM
UT_Data as d
INNER JOIN UT_Indicator_Classifications_%lang as s on s.IC_NId = d.Source_NId
"""

Q_SOURCE_AGENCY = """\
Select cat.id as Indicator_NId, cat.SOURCE_AGENCY as SOURCE_AGENCY FROM
(select DISTINCT i.Indicator_NId as id, c.IC_Name as SOURCE_AGENCY from (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'SR' and c.IC_Parent_NId = -1) as cat
"""

Q_AGENCY = """\
Select cat.id as Indicator_NId, cat.AGENCY as AGENCY FROM
(select DISTINCT i.Indicator_NId as id, c.IC_Name as AGENCY from (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'IT') as cat
"""

Q_GOALS = """\
Select cat.id as Indicator_NId, cat.GOALS as GOALS FROM
(select DISTINCT i.Indicator_NId as id, c.IC_Name as GOALS from (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'GL' and c.IC_Parent_NId <> -1) as cat
"""

Q_THEMES = """\
Select cat.id as Indicator_NId, cat.THEMES as THEMES FROM
(select DISTINCT i.Indicator_NId as id, c.IC_Name as THEMES from (((UT_Indicator_%lang as i
INNER JOIN UT_Indicator_Unit_Subgroup as evil on evil.Indicator_NId = i.Indicator_NId)
INNER JOIN UT_Indicator_Classifications_IUS as evil2 on evil2.IUSNId = evil.IUSNId)
INNER JOIN UT_Indicator_Classifications_%lang as c on c.IC_NId = evil2.IC_NId)
where c.IC_Type = 'TH' and c.IC_Parent_NId <> -1) as cat
"""


class DbRead:
    database_path = None
    connStr = (
        r"Driver={{Microsoft Access Driver (*.mdb)}};"
        r"DBQ={};"
    )

    def __init__(self, database_path):
        DbRead.database_path = database_path

    def get_dataframe(self, query, language):
        q_lang = query.replace(lang_placeholder, language)
        cnxn = pyodbc.connect(DbRead.connStr.format(DbRead.database_path))
        df = pd.read_sql(q_lang, cnxn)
        cnxn.close()
        return df
