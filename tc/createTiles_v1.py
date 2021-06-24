"""
Script: createTile_v1.py
Purpose: Create a buffered grid for a county using the county's psegs to get an estimate of segments
         per tile in the grid. These tiles are required for the TC submodule.
Author: Sarah McDonald, Geographer, U.S. Geological Survey
Contact: smcdonald@chesapeakebay.net
"""
import geopandas as gpd 
from shapely.geometry import Polygon
import numpy as np 
import luconfig as config

def createTiles(cf, psegs):
    """
    Method: createTiles()
    Purpose: Create tiles for a county where each tile contains psegs within the range specified
             in the luconfig file.
    Params: cf - county fips
            psegs - gdf of county psegs
    Returns: N/A
    """
    test_gpkg = f'{config.folder}/{cf}/output/data.gpkg'
    grid_p = f'{config.folder}/{cf}/temp/tc_tiles.shp'

    if len(psegs) <= config.TC_Tile_Max+50000: # if whole county is small - only need 1 tile covering county
        minx, miny, maxx, maxy = psegs.total_bounds
        poly = Polygon([(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy)])
        tiles = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:5070")
        tiles.loc[:, 'id'] = [1]
        tiles.loc[:, 'cnt'] = [len(psegs)]
        tiles[['id', 'cnt', 'geometry']].to_file(grid_p)
    else: # county needs multiple tiles
        # Create spatial index
        psegs_sidx = psegs.sindex
        start_bnds = psegs.total_bounds
        del psegs

        #Create starter grid - START WITH BIG TILES
        grid = createBaseGrid(start_bnds, 75)

        #For each tile in grid - get intersection of psegs idx - this needs to be done each time a tile size changes
        hits_list = []
        for idx, row in grid.iterrows(): # do all tiles the first time - if you update a tile, you don't need to loop all tiles
            bounds = row['geometry'].bounds
            hits_list.append(len(list(psegs_sidx.intersection(bounds))))

        #Add number of pseg hits in each box - each time a tile is updated
        grid.loc[:, 'cnt'] = hits_list 
        grid = grid[grid.cnt != 0]

        #Get list of tiles who are too small
        small_tiles = list(grid[grid['cnt'] <= config.TC_Tile_Min]['id'])
        #For all small tiles, determine what tiles to dissolve to meet threshold
        if len(small_tiles) > 0:
            #Find direct neighbors of tiles that are too small
            grid = smallTileDissolve(small_tiles, grid, psegs_sidx)

        # Split large tiles
        grid = splitTiles(75, grid, psegs_sidx)
        grid = grid[grid['cnt'] > 0]
        grid['cnt'] = grid.cnt.astype(int)
        grid = grid.reset_index()
        grid[['id', 'cnt', 'geometry']].to_file(grid_p)

def createBaseGrid(bounds, bufDist):
    """
    Method: createGrid()
    Purpose: Create a temporary grid of boxes with dimensions wSpace x hSpace, each increased
             by buffer distance (bufDist).
    Params: bounds - tuple of county boundary (minx, miny, maxx, maxy)
            wSpace - width in meters each tile should be
            hSpace - height in meters each tile should be
            bufDist - buffer distance in meters
    Returns: tiles - gdf of tile geometries and row, col columns
    """
    minx, miny, maxx, maxy = bounds
    width, height = maxx - minx, maxy - miny
    w_range, h_range = 3, 3 # numb of tiles
    wSpace, hSpace = int(width/w_range), int(height/h_range)
    grid, r_list, c_list = [], [], []
    for i in range(0, int(w_range)):# + 1):
        for j in range(0, int(h_range)):# + 1):
            mn_x, mn_y, mx_x, mx_y = minx+(i*wSpace)-bufDist, miny+(j*hSpace)-bufDist, minx+((i+1)*wSpace)+bufDist, miny+((j+1)*hSpace)+bufDist
            poly = Polygon([(mn_x, mn_y), (mx_x, mn_y), (mx_x, mx_y), (mn_x, mx_y)])
            grid.append(poly)
            r_list.append(i)
            c_list.append(j)
    tiles = gpd.GeoDataFrame(geometry=grid, crs="EPSG:5070")
    tiles.loc[:, 'row'] = r_list
    tiles.loc[:, 'col'] = c_list
    tiles.loc[:, 'id'] = [int(x) for x in range(1, len(tiles)+1)]
    return tiles

def getNeighborsDict(grid):
    """
    Method: getNeighborsDict()
    Purpose: Build a dictionary of tile IDs where the data for each tile is another dictionary of:
                row: list of tiles ids that are neighbors in the same row
                col: list of tiles ids that are neighbors in the same col
    Params: grid - gdf of tiles
    Returns: neighbors_dict - dictionary of tile neighbors
    """
    neighbors_dict = {}
    for idx, row in grid.iterrows():
        curr_row = row['row']
        curr_col = row['col']
        row_nbrs = list(grid[(grid['row']==curr_row)&(grid['col'].isin([curr_col-1, curr_col+1]))]['id']) # row neighbors
        col_nbrs = list(grid[(grid['col']==curr_col)&(grid['row'].isin([curr_row-1, curr_row+1]))]['id']) # col neighbors
        neighbors_dict[row['id']] = {'row':row_nbrs,
                                     'col':col_nbrs}
    return neighbors_dict

def smallTileDissolve(small_tiles, grid, psegs_sidx):
    """
    Method: smallTileDissolve()
    Purpose: For tiles that are too small, find what tiles need to be dissolved to exceed the min threshold.
             If the tiles will exceed max threshold after dissolve, note which direction to split the dissolved 
             tile in. THIS CODE ONLY WORKS IF ORIGINAL GRID IS 3X3
    Params: small_tiles - list of tile IDs whose seg count is too low
            grid - gdf of tiles
            neighbors_dict - dictionary of tile neighbors
    Returns: grid - gdf of tiles that needed to be dissolved; they are dissolved with 'split' field to tell you
                     which direction they were dissolved in.
    """
    neighbors_dict = getNeighborsDict(grid)
    row_dissolve_all, col_dissolve_all, row_counts, col_counts, rowFlags, colFlags = [],[],[],[],[],[]
    for tile in small_tiles: # get dissolve options for all small tiles
        row_dissolve, rowSplitFlag, fin_row_cnt = smallTileHelper('row', tile, neighbors_dict, grid, small_tiles)
        col_dissolve, colSplitFlag, fin_col_cnt = smallTileHelper('col', tile, neighbors_dict, grid, small_tiles)
        row_dissolve_all.append(row_dissolve)
        col_dissolve_all.append(col_dissolve)
        row_counts.append(fin_row_cnt)
        col_counts.append(col_counts)
        rowFlags.append(rowSplitFlag)
        colFlags.append(colSplitFlag)

    useRow = []
    for i in range(len(small_tiles)):
        if len(row_dissolve_all[i]) == 0: # can only dissolve by col
            if len(col_dissolve_all[i]) == 0:
                useRow.append(None) # not touching anything????
            else:
                useRow.append(False)
        elif len(col_dissolve_all[i]) == 0: # can only dissolve by row
            useRow.append(True)
        else:
            useRow.append(None)
    tiles_to_remove, off_limits = [], []
    for i in range(len(useRow)):
        if useRow[i] == True:
            if i not in tiles_to_remove:
                t = row_dissolve_all[i][1:len(row_dissolve_all[i])] # first tile is always the current small tile
                off_limits += row_dissolve_all[i]
                ignore = list(set(t) & set(small_tiles))
                if len(ignore) > 0:
                    for j in ignore:
                        tiles_to_remove.append(small_tiles.index(j)) # tiles that are already taken care of
        elif useRow[i] == False:
            if i not in tiles_to_remove:
                t = col_dissolve_all[i][1:len(col_dissolve_all[i])] # first tile is always the current small tile
                off_limits += col_dissolve_all[i]
                ignore = list(set(t) & set(small_tiles))
                if len(ignore) > 0:
                    for j in ignore:
                        tiles_to_remove.append(small_tiles.index(j)) # tiles that are already taken care of

    tiles_to_remove = list(set(tiles_to_remove))
    off_limits = set(off_limits)
    tiles_to_remove = sorted(tiles_to_remove, reverse=True)
    for rem in tiles_to_remove: # tiles that were too small but are already dealt with in another group
        del useRow[rem]
        del row_dissolve_all[rem]
        del col_dissolve_all[rem]
        del row_counts[rem]
        del col_counts[rem]
        del rowFlags[rem]
        del colFlags[rem]
    if None in useRow: # tiles that can be dissolved either direction and are not required by another tile
        for u in range(len(useRow)):
            if useRow[u] == None:
                # check if either direction is off limits
                if list(set(set(row_dissolve_all[u]) & set(off_limits))) == 0: # can use row
                    if list(set(set(col_dissolve_all[u]) & set(off_limits))) > 0: # can't use col
                        useRow[u] = True
                    else: # can use both - use seg counts to choose which way to go
                        if rowFlags[u] == True and colFlags[u] == False: # have to dis row not col
                            useRow[u] = False
                        elif rowFlags[u] == False and colFlags[u] == True: # have to dis col not row
                            useRow[u] = True
                        else: # pick largest cnt to split or use as dis
                            if row_counts[u] > col_counts[u]:
                                useRow[u] = True
                            else:
                                useRow[u] = False
                elif list(set(set(col_dissolve_all[u]) & set(off_limits))) == 0: # can't use row can use col
                    useRow[u] = False
    for i in range(len(useRow)):
        if useRow[i] == True:
            s = rowFlags[i]
            if s:
                s = 'dissolved rows'
            else:
                s = 'dissolved cols'
            grid.loc[grid['id'].isin(row_dissolve_all[i]), 'GID'] = i + 1
            grid.loc[grid['id'].isin(row_dissolve_all[i]), 'split'] = s
        else:
            s = colFlags[i]
            if not s:
                s = 'dissolved rows'
            else:
                s = 'dissolved cols'
            grid.loc[grid['id'].isin(col_dissolve_all[i]), 'GID'] = i + 1
            grid.loc[grid['id'].isin(col_dissolve_all[i]), 'split'] = s

    if 'GID' in list(grid):
        oldgrid = grid[grid['GID'].isna()]
        cols = list(oldgrid)
        cols.remove('GID')
        grid = grid[~grid['GID'].isna()]
        grid = grid.dissolve(by='GID')
        hits_list = []
        for idx, row in grid.iterrows(): # do all tiles the first time - if you update a tile, you don't need to loop all tiles
            bounds = row['geometry'].bounds
            hits_list.append(len(list(psegs_sidx.intersection(bounds))))
        #Add number of pseg hits in each box - each time a tile is updated
        grid.loc[:, 'cnt'] = hits_list 
        grid = grid[grid['cnt'] > 0]

        return grid.reset_index()[cols].append(oldgrid[cols])
    else:
        return grid

def smallTileHelper(rowColStr, tile, neighbors_dict, grid, small_tiles):
    """
    Method: smallTileHelper()
    Purpose: Build list of tiles in 1 direction (row or col) of tiles needed to exceed min threshold.
    Params: rowColStr - 'col' or 'row'
            tile - current tile ID
            neighbors_dict - dict of neighbors
            grid - gdf of tiles
            small_tiles - lsit of tiles that are small
    Returns: row_dissolve - list of tile ids to dissolve to exceed min threshold
            splitFlag - True if tiles exceed max threshold
            fin_row_cnt - total pseg count
    """
    row_nbrs = neighbors_dict[tile][rowColStr]
    row_dissolve, splitFlag, fin_row_cnt = [], False, 0
    if len(row_nbrs) > 0:
        start_cnt = list(grid[grid['id']==tile]['cnt'])[0]
        other_small = list(set(small_tiles)&set(row_nbrs)) # intersection of small tiles in same row
        if len(other_small) > 0: # if there are other small tiles dissolve them first
            tot_cnt = start_cnt + list(grid[grid['id']==other_small[0]]['cnt'])[0]
            if len(other_small) == 2:
                tot_cnt += list(grid[grid['id']==other_small[1]]['cnt'])[0]
                # if tot_cnt > config.TC_Tile_Min:
                row_dissolve = row_nbrs+[tile]
                fin_row_cnt = tot_cnt
            else:
                # if tot_cnt > config.TC_Tile_Min:
                row_dissolve = other_small+[tile]
                fin_row_cnt = tot_cnt
                if len(row_nbrs) == 2: # 2 of the 3 tiles are small - dissolve all 3
                    otherID = list(set(row_nbrs) - set(other_small))[0]
                    if tot_cnt < config.TC_Tile_Min:
                        tot_cnt += list(grid[grid['id']==otherID]['cnt'])[0]
                        # if tot_cnt > config.TC_Tile_Min:
                        row_dissolve = row_nbrs+[tile]
                        fin_row_cnt = tot_cnt
                        if tot_cnt > config.TC_Tile_Max:
                            splitFlag = True        
        else:
            dis = []
            if len(row_nbrs) == 1:
                cnt1 = start_cnt + list(grid[grid['id']==row_nbrs[0]]['cnt'])[0]
                dis = [tile, row_nbrs[0]]
                fin_row_cnt = cnt1
                if cnt1 > config.TC_Tile_Max:
                    splitFlag = True
            elif len(row_nbrs) == 2:
                # check both neighbors to see if dis with 1 will fit range
                cnt1 = start_cnt + list(grid[grid['id']==row_nbrs[0]]['cnt'])[0]
                cnt2 = start_cnt + list(grid[grid['id']==row_nbrs[1]]['cnt'])[0]
                if cnt1 < config.TC_Tile_Max: # cnt1 in range
                    if cnt2 < config.TC_Tile_Max: # cnt2 in range
                        if cnt2 - start_cnt + cnt1 < config.TC_Tile_Max: # can dis all 3
                            dis = [tile, row_nbrs[0], row_nbrs[1]]
                            fin_row_cnt = cnt2 - start_cnt + cnt1
                            split = False
                        elif cnt2 > cnt1: # cnt2 is higher
                            dis = [tile, row_nbrs[1]]
                            fin_row_cnt = cnt2
                            splitFlag = False
                        else: # cnt1 is higher
                            dis = [tile, row_nbrs[0]]
                            fin_row_cnt = cnt1
                            splitFlag = False
                    else: # cnt1 in range, cnt2 not
                        dis = [tile, row_nbrs[0]]
                        fin_row_cnt = cnt1
                        splitFlag = False 
                elif cnt2 < config.TC_Tile_Max: # cnt2 in range: # cnt1 not in range
                    dis = [tile, row_nbrs[1]]
                    fin_row_cnt = cnt2
                    splitFlag = False
                else: # neither in range, pick the largest?
                    if cnt2 < config.TC_Tile_Min and cnt1 < config.TC_Tile_Min:
                            dis = [tile, row_nbrs[0], row_nbrs[1]]
                            fin_row_cnt = cnt2 - start_cnt + cnt1
                            splitFlag = False
                    elif cnt2 > cnt1: # cnt2 is higher
                        dis = [tile, row_nbrs[1]]
                        fin_row_cnt = cnt2
                        splitFlag = False
                    else: # cnt1 is higher
                        dis = [tile, row_nbrs[0]]
                        fin_row_cnt = cnt1
                        splitFlag = False                    

            row_dissolve = dis
        
        return row_dissolve, splitFlag, fin_row_cnt
    print("No neighbors: ", tile, rowColStr, row_nbrs)
    return [], False, 0

def splitTiles(bufDist, grid, psegs_sidx):
    """
    Method: splitTiles()
    Purpose: Once all tiles have been dissolved to exceed the minumum threshold, this function
             will split tiles exceeding the maximum threshold vertically (or horizontally if the
             tile was dissolved horizontally/row). The tile will continue to be split in equal increments
             until all pieces are under the maximum. The pieces are then passed back to smallTileDissolve()
             to re-dissolve the chunks that became too small.
    Params: bufDist - integer buffer distance for the tiles (75 meters)
            grid - geodataframe of dissolved tiles
            psegs_sidx - spatial indexing of psegs layer
    Returns: grid - updated geodataframe of final tiles for the county
    """
    bad_tiles = grid[grid['cnt'] >= config.TC_Tile_Max]
    grid = grid[grid['cnt'] < config.TC_Tile_Max]
    splitColumn = False
    if 'split' in list(grid):
        splitColumn = True
    for idx, row in bad_tiles.iterrows():
        fixed_tiles = gpd.GeoDataFrame()
        minx, miny, maxx, maxy = row['geometry'].bounds
        if splitColumn and row['split'] == 'dissolved rows':
            splitCols = True
        else:
            splitCols = False # always split by row unless rows were dissolved
        i = 1
        while len(fixed_tiles) == 0:
            i += 1
            newBounds = []
            if splitCols:
                height_dif = (maxy - miny) / i
                for j in range(1, i+1):
                    if j == 1:
                        newBounds.append((minx, miny, maxx, miny+height_dif+bufDist))
                    elif j == i:
                        newBounds.append((minx, miny+(height_dif*(j-1))-bufDist, maxx, maxy))
                    else:
                        newBounds.append((minx, miny+(height_dif*(j-1))-bufDist, maxx, miny+(height_dif*j)+bufDist))
            else:
                length_dif = (maxx - minx) / i
                for j in range(1, i+1):
                    if j == 1:
                        newBounds.append((minx, miny, minx+length_dif+bufDist, maxy))
                    elif j == i:
                        newBounds.append((minx+(length_dif*(j-1))-bufDist, miny, maxx, maxy))
                    else:
                        newBounds.append((minx+(length_dif*(j-1))-bufDist, miny, minx+(length_dif*j)+bufDist, maxy))
            polys, hits_list = [], []
            validFlag = True
            for j in newBounds:
                polys.append(Polygon([(j[0], j[1]), (j[2], j[1]), (j[2], j[3]), (j[0], j[3])]))
                hits_list.append(len(list(psegs_sidx.intersection(j))))
                if hits_list[len(hits_list)-1] > config.TC_Tile_Max:
                    validFlag = False
            if validFlag:
                fixed_tiles.loc[:, 'geometry'] = polys
                fixed_tiles.loc[:, 'cnt'] = hits_list
                fixed_tiles = fixed_tiles[fixed_tiles['cnt'] > 0]
                fixed_tiles.crs = 'EPSG:5070'
                st_id = int(np.amax(grid['id'])) + 1
                fixed_tiles.loc[:, 'id'] = [int(x) for x in range(st_id, st_id+len(fixed_tiles))]
                fixed_tiles.loc[:, 'row'] = 1
                fixed_tiles.loc[:, 'col'] = [int(i) for i in range(1, len(fixed_tiles)+1)]
                fixed_tiles = splitTileHelper(fixed_tiles, psegs_sidx)
                grid = grid.append(fixed_tiles)
                break
    grid.loc[:, 'id'] = [int(x) for x in range(1, len(grid)+1)]
    return grid

def splitTileHelper(fixed_tiles, psegs_sidx):
    """
    Method: splitTileHelper()
    Purpose: Loop through newly split tiles and dissolved as needed
    Params: fixed_tiles - gdf of newly split tile
            psegs_sidx - spatial index of psegs
    Returns: fixed_tiles - gdf of dissolved tiles
    """
    dis_list, cur_dis = [], []
    all_tiles = list(fixed_tiles['id'])
    totCnt = 0
    for s in all_tiles: # loop through tiles
        cur_cnt = list(fixed_tiles[fixed_tiles['id']==s]['cnt'])[0] # get current tile count

        if totCnt + cur_cnt < config.TC_Tile_Max + 100000: # can add current tile to current dissolve list
            cur_dis.append(s)
            totCnt += cur_cnt # add current tile count to total
            if all_tiles.index(s) == len(all_tiles) - 1: # added last tile to group
                if len(cur_dis) > 1: # save group if it exists
                    dis_list.append(cur_dis)
                elif cur_cnt == totCnt and cur_cnt < config.TC_Tile_Min: # final tile needs a group
                    prev = s - 1
                    if prev in dis_list[-1]: # if the last group contains the previous tile, add the end tile
                        dis_list[-1].append(s)
                    else: # create group of last 2 tiles --  this may re-dissolve a tile, but it is ok since thresholds include a buffer
                        dis_list.append([prev, s])
        else: # cannot add current to current group
            if len(cur_dis) > 1: # there is a dissolve group that exists
                if all_tiles.index(s) == len(all_tiles) - 1 and cur_cnt < config.TC_Tile_Min: # current tile needs to be dissolved and is on the end
                    cur_dis.append(s)
                dis_list.append(cur_dis)
                cur_dis = []
                totCnt = 0
            else: # current group is single tile; replace with this tile
                cur_dis = [s]
                totCnt = cur_cnt
    
    # do the dissolves
    dis_ids = []
    for d in dis_list:
        dis_ids += d
    grid = fixed_tiles[fixed_tiles['id'].isin(dis_ids)] # need to dissolve
    fixed_tiles = fixed_tiles[~fixed_tiles['id'].isin(dis_ids)] # don't dissolve
    for d in range(len(dis_list)):
        grid.loc[grid['id'].isin(dis_list[d]), 'GID'] = d + 1
    if len(dis_list) > 0:
        grid = grid.dissolve(by='GID')
        grid = grid.reset_index()
        hits_list = []
        for idx, row in grid.iterrows(): # do all tiles the first time - if you update a tile, you don't need to loop all tiles
            bounds = row['geometry'].bounds
            hits_list.append(len(list(psegs_sidx.intersection(bounds))))
        #Add number of pseg hits in each box - each time a tile is updated
        grid.loc[:, 'cnt'] = hits_list 
        cols = list(fixed_tiles)
        grid = grid[grid['cnt'] > 0]
        fixed_tiles = fixed_tiles.append(grid[cols])
    return fixed_tiles
