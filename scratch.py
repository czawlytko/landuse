    joinColumns = ['p_area', 's_area', 'p_lc_1', 'p_lc_3', 'p_lc_4', 'p_lc_5', 'p_lc_6', 'p_lc_7', 'p_lc_8', 'p_lc_9', 'p_lc_10', 'p_lc_11', 'p_lc_12',
        's_c18_0', 'p_c18_0', 's_c1719_0', 's_n16_0', 'p_luz','s_luz']
    jc = 0
    for col in joinColumns:
        if col not in psegs.columns:
            jc += 1
    if jc != 0:
        print('Required join column(s) missing, trying to join data')
        print(psegs.columns)
        print(joinColumns)
        psegs = psegs[['PSID', 'PID', 'SID', 'Class_name', 'geometry']]
        psegs = joinData(cf, psegs, True) # replace remove_columns bool with column check

    for col in joinColumns:
        if col not in psegs.columns:
            print(f"Required columns still missing.\npseg cols:{psegs.columns}\nRequired joined cols: {joinedColumns}")
            sys.exit()


