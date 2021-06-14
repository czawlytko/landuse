import xml.etree.ElementTree as ET
import pandas as pd 

def buildChangeData(xmlPath):
    """
    Method: buildChangeData()
    Purpose: Read in county change XML file to associate raster values to description of change.
    Params: xmlPath - path to xml file
    Returns: df - dataframe with fields Value and Description
    """
    # read in xml file
    tree = ET.parse(xmlPath)
    # find starting point
    root = tree.getroot()
    # create list of objects that hold raster values and descriptions ('edom')
    eainfo = root.find('eainfo')
    detailed = eainfo.findall('detailed')
    allAttr = []
    for d in detailed:
        allAttr += d.findall('attr')
    attrdomv = []
    for a in allAttr:
        attrdomv += a.findall('attrdomv')
    edom = []
    for a in attrdomv:
        edom += a.findall('edom')
    # build lists of values and descriptions
    vals, desc = [], []
    for e in edom:
        vals.append(int(e.find('edomv').text))
        desc.append(e.find('edomvd').text)
    # store data in pandas df
    df = pd.DataFrame(data={'Value':vals, 'Description':desc})
    # return data
    return df

def defineReclassValues(df):
    """
    Method: defineReclassValues()
    Purpose: Using LC Values and description field from XML document, determine
             the reclass crosswalk to create 2013/2014 LC.
    Params: df - dataframe of change description and values
    Returns: df - original dataframe with added NewVal column
    """
    #dictionary of base land cover values
    lc_reclass_dict = {
            'Water':	1,
            'Emergent Wetlands':	2,
            'Tree Canopy':	3,
            'Scrub\Shrub':	4,
            'Low Vegetation':	5,
            'Barren':	6,
            'Structures':	7,
            'Other Impervious Surfaces':	8,
            'Roads':	9,
            'Tree Canopy Over Structures':	10,
            'Tree Canopy Over Other Impervious Surfaces':	11,
            'Tree Canopy Over Roads':	12
    }
    
    new_vals = [[] for i in range(len(lc_reclass_dict))] #create empty list of lists with same # of rows as lc values
    # for each description in the XML - get the first lc type
    for idx, row in df.iterrows():
        words = row['Description'].split(' ') #separate by space
        if words[0] == 'Water': 
            new_vals[0].append(row['Value'])
        elif words[0] == 'Emergent':
            new_vals[1].append(row['Value'])
        elif words[0] == 'Scrub\Shrub':
            new_vals[3].append(row['Value'])
        elif words[0] == 'Low':
            new_vals[4].append(row['Value'])
        elif words[0] == 'Barren':
            new_vals[5].append(row['Value'])
        elif words[0] == 'Structures':
            new_vals[6].append(row['Value'])
        elif words[0] == 'Other':
            new_vals[7].append(row['Value'])
        elif words[0] == 'Roads':
            new_vals[8].append(row['Value'])
        else: # one of the tree canopy classes
            if len(words) == 2 or words[2] == 'to':
                new_vals[2].append(row['Value'])
            elif words[3] == 'Structures':
                new_vals[9].append(row['Value'])
            elif words[3] == 'Other':
                new_vals[10].append(row['Value'])
            elif words[3] == 'Roads':
                new_vals[11].append(row['Value'])

    # update df with new col with new reclass value
    for i in range(len(new_vals)):
        df.loc[df['Value'].isin(new_vals[i]), 'NewVal'] = int(i+1)

    #return updated data
    return df
