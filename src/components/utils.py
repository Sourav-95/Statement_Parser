from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from typing import List
import os, shutil
import platform

def parallel_apply_process(df, func, column, max_workers=8):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(func, df[column]), total=len(df)))
    return results

def get_file_nm_list(items:List):
    set_nm = []
    if items:
        for nm in items:
            split_nm = os.path.basename(nm)
            set_nm.append(split_nm)
        return set_nm
    return items

def remove_temp_dir(dir):
    del_status = None
    if os.path.exists(dir):
        shutil.rmtree(dir)
        del_status = 1
        
    return del_status

