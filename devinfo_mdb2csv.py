import sys
import pandas as pd
import numpy as np
import time as time
import csv

import db_read


# "Z:\devinfo\cam\CAMInfo 2016_r8_DECRY
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#
# "Z:\devinfo\cam\CAMInfo 2016_r8_DECRYPT.csv" km


# Subgroup_Type_Name distinct
def get_subgroup_name_distinct(df):
    df_subgroup_name = df['Subgroup_Type_Name']
    df_subgroup_name.drop_duplicates(inplace=True)
    return df_subgroup_name


'''
This function gets the Dimensions table in the following form:
Subgroup_Val_NId    Subgroup_Type_Name  Subgroup_Name
1                   Location            Total
2                   Sex                 Male
2                   Age                 65+ yr
3                   Sex                 Male
3                   Age                 55-64 yr

And returns a table in the following form:
Subgroup_Val_NId    Location    Sex     Age         Other
1                   Total       NaN     NaN         NaN
2                   NaN         Male    65+ yr      NaN
3                   NaN         Male    55-64 yr    NaN

Creating a column for each Subgroup_Type_Name and adding the proper value when available
'''


def create_dimensions_table(df):
    # gets the unique values of the Subgroup_Type_Name
    df_subgroup_name = get_subgroup_name_distinct(df)
    # creates the new set of columns in the Pandas dataframe: an id column + all the Unique subgroups found
    new_cols = {'Subgroup_Val_NId': np.int64}
    for c in df_subgroup_name:
        new_cols.update({c: np.str})
    df_dims = pd.DataFrame(columns=new_cols)

    '''
    For each row: add the value in the right column
    Get the id (val_nid)
    if exists get the location if not create a new one
    get the row (line)
    get the name contained in the Subgroup_Type_Name column (dim_name)
    get the value contained in the Subgroup_Name column (dim_value)
    add the value in the right column
    '''
    for i in range(0, len(df)):
        val_nid = df.iloc[i, 0]  # df.iloc gets the row i col 0
        exists = df_dims.loc[df_dims['Subgroup_Val_NId'] == val_nid]
        if exists.empty:
            df_dims = df_dims.append({'Subgroup_Val_NId': val_nid}, ignore_index=True)
        line = df.iloc[i]
        dim_name = line[1]
        dim_value = line[2]
        df_dims.loc[df_dims['Subgroup_Val_NId'] == val_nid, dim_name] = dim_value
    return df_dims


'''Extracts the facts table and merges all the additional information
Renames the columns to hanlde the multilanguage
the dataFrameName.groupby('Indicator_NId').apply(lambda x: ','.join(x.COLUMN_NAME)) is used to join all the values having the same id in one row, separated by comma
e.g. 
id:25 SUBSECTOR: Primary Education
id:25 SUBSECTOR: Secondary Education
id:25 SUBSECTOR: Upper Secondary Education
Becomes
id:25 SUBSECTOR: Primary Education, Secondary Education, Upper Secondary Education

The operation doesn't seem to be available in Access so it is done in Pandas
'''


def extract_dataframe(access_database, lang):
    db = db_read.DbRead(access_database)

    print("Extracting data from the database")
    # Extract the facts and dimensions data from the database
    dfFacts = db.get_dataframe(db_read.Q_FACTS, lang)
    dfDims = db.get_dataframe(db_read.Q_DIMS, lang)
    # creates the dimensions table
    dfAllDims = create_dimensions_table(dfDims)

    # extracts the sectors table
    dfSectors = db.get_dataframe(db_read.Q_SECTORS, lang)
    if len(dfSectors) > 0:
        dfSectors = dfSectors.groupby('Indicator_NId').apply(lambda x: ','.join(x.SECTOR))
        tmpdf = pd.DataFrame()
        tmpdf['SECTOR'] = dfSectors
        dfSectors = tmpdf

    # extracts the subsectors table
    dfSubsectors = db.get_dataframe(db_read.Q_SUBSECTORS, lang)
    if len(dfSubsectors) > 0:
        dfSubsectors = dfSubsectors.groupby('Indicator_NId').apply(lambda x: ','.join(x.SUBSECTOR))
        tmpdf = pd.DataFrame()
        tmpdf['SUBSECTOR'] = dfSubsectors
        dfSubsectors = tmpdf

    # extracts the sources table
    dfSources = db.get_dataframe(db_read.Q_SOURCES, lang)

    # extracts the source agency table
    dfSource_agency = db.get_dataframe(db_read.Q_SOURCE_AGENCY, lang)
    if len(dfSource_agency) > 0:
        dfSource_agency = dfSource_agency.groupby('Indicator_NId').apply(lambda x: ','.join(x.SOURCE_AGENCY))
        tmpdf = pd.DataFrame()
        tmpdf['SOURCE_AGENCY'] = dfSource_agency
        dfSource_agency = tmpdf

    # extracts the agency table
    dfAgency = db.get_dataframe(db_read.Q_AGENCY, lang)
    if len(dfAgency) > 0:
        dfAgency = dfAgency.groupby('Indicator_NId').apply(lambda x: ','.join(x.AGENCY))
        tmpdf = pd.DataFrame()
        tmpdf['AGENCY'] = dfAgency
        dfAgency = tmpdf

    # extracts the goals table
    dfGoals = db.get_dataframe(db_read.Q_GOALS, lang)
    if len(dfGoals):
        dfGoals = dfGoals.groupby('Indicator_NId').apply(lambda x: ','.join(x.GOALS))
        tmpdf = pd.DataFrame()
        tmpdf['GOALS'] = dfGoals
        dfGoals = tmpdf

    # extracts the themes table
    dfThemes = db.get_dataframe(db_read.Q_THEMES, lang)
    if len(dfThemes) > 0:
        dfThemes = dfThemes.groupby('Indicator_NId').apply(lambda x: ','.join(x.THEMES))
        tmpdf = pd.DataFrame()
        tmpdf['THEMES'] = dfThemes
        dfThemes = tmpdf

    print("mapping...")
    # A set of joins to generate the final table
    dfFacts['Subgroup_Val_NId'] = dfFacts['Subgroup_Val_NId'].astype(np.int64)
    dfFacts['Indicator_NId'] = dfFacts['Indicator_NId'].astype(np.int64)
    dfFacts = pd.merge(dfFacts, dfSectors, on='Indicator_NId', how='left')
    dfFacts = pd.merge(dfFacts, dfSubsectors, on='Indicator_NId', how='left')
    dfFacts = pd.merge(dfFacts, dfSources, on='data_id', how='left')
    dfFacts = pd.merge(dfFacts, dfSource_agency, on='Indicator_NId', how='left')
    dfFacts = pd.merge(dfFacts, dfAgency, on='Indicator_NId', how='left')
    dfFacts = pd.merge(dfFacts, dfGoals, on='Indicator_NId', how='left')
    if dfThemes.empty:
        dfFacts['THEMES'] = ""
    else:
        dfFacts = pd.merge(dfFacts, dfThemes, on='Indicator_NId', how='left')

    dfFacts = pd.merge(dfFacts, dfAllDims, how='left', left_on='Subgroup_Val_NId', right_on='Subgroup_Val_NId')
    dimCols = get_subgroup_name_distinct(dfDims)

    # re arrange the columns: the first set of columns (always present)
    cols_position = ['data_id', 'SECTOR', 'SUBSECTOR', 'GOALS', 'THEMES', 'AGENCY', 'SOURCE_AGENCY', 'SOURCE',
                     'INDICATOR']
    # the ones generated by "splitting" the Subgroup_Type_Name
    cols_position.extend(dimCols.values.tolist())
    # the last ones (always present).
    cols_position.extend(
        ['REF_AREA', 'REF_AREA_CODE', 'REF_AREA_TYPE', 'TIME_PERIOD', 'UNIT_MEASURE', 'OBS_VALUE', 'FOOTNOTE'])

    # re arrange the columns
    dfFacts = dfFacts[cols_position]
    # rename the columns, data_id and OBS_VALUE are not renamed, append the language code to the other ones
    newColNames = []
    for c in dfFacts.columns:
        if (c == 'data_id' or c == 'OBS_VALUE'):
            newColNames.append(c)
        else:
            newColNames.append(c + "_" + lang)

    # rename facts ID col from data_id to id
    newColNames[0] = 'id'

    dfFacts.columns = newColNames #assign the new col names
    return dfFacts


def main(argv):
    # Check and parse the parameters
    if len(argv) < 3:
        print("Use: python.exe devinfo_mdb2csv.py source destination <second language>")
        return

    access_database = argv[1]
    output_csv = argv[2]

    lang1 = 'en'
    lang2 = None
    if (len(argv) == 4):
        if (len(argv[3]) == 2):
            lang2 = argv[3]
        else:
            print("The additional language must be the two letter code: e.g. sp")
            return

    print("reading " + access_database + "...")

    # pandas options used for debug reasons to show all the dataframe can be deleted
    pd.set_option('display.expand_frame_repr', False)

    time1 = time.time()

    # calls the extract datafextract_dataframe function to retrieve the data to be written to CSV
    dfFacts = extract_dataframe(access_database, lang1)

    # repeats the extraction if a second language is available (and passed as param)
    if lang2 is not None:
        print('extracting ' + lang2 + ' language from database...')
        dfFacts2 = extract_dataframe(access_database, lang2)
        dfFacts2.drop(columns=['OBS_VALUE'], axis=1, inplace=True)
        dfFacts = pd.merge(dfFacts, dfFacts2, how='inner', left_on='id', right_on='id')

    print("saving to " + output_csv + "...")
    dfFacts.to_csv(output_csv, index=False, sep=",", quoting=csv.QUOTE_NONNUMERIC, quotechar='"',
                   encoding='utf-8')

    time2 = time.time()
    print("done.")
    print(str(round((time2 - time1), 1)) + " seconds")


if __name__ == "__main__":
    main(sys.argv)
