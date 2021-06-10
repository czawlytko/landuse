import TC_LU_Submodule_v1 as trees_over

if __name__ == "__main__":
    cf = 'loud_51107'
    NUM_CPUS = 5

    tc_flag = trees_over.run_trees_over_submodule(NUM_CPUS, cf)

    if tc_flag == 0:
        print("Trees over Submodule complete")
    elif tc_flag == -1:
        print("Trees over Submodule incomplete; Check log for error")
    else:
        print("Trees over Submodule flag invalid value: ", tc_flag)
        print("Check for trees_over.gpkg manually")