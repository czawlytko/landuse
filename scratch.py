def runlu():
    try:
        df
        print('ran')
        psegs = [1,2,3]

        flag = 1
        return flag, psegs
    except:
        
        print('except')
        flag = -1
        psegs = 'did not return psegs'
        return flag, psegs

lu_flag, psegs = runlu()

print(psegs)
print(lu_flag)