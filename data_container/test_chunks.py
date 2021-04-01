#!/usr/bin/env python3

from data_container import *
from datetime import datetime
import time
import json

# set SLICE_MAX_SIZE = 48

def testing():
    
    t_start = time.monotonic()
    df = DataFile()
    df._meta['date_time_start'] = datetime.now()
    for i in range(100):
        if i % 22 == 0:
            print(df.chunks)
            if df.chunks:
                df.chunk_stop()
                print(df.chunks[-1])
            df.chunk_start()
        df.append_value('heart_rate', i, time.monotonic()-t_start)
        df.append_value('rr_int', 10*i, time.monotonic()-t_start)
        time.sleep(0.1)
    df.chunk_stop()
    
    print(df)
    
    for chunk in df.chunks:
        print('###################################################')
        print(json.dumps(chunk.dump(), indent=4, default=str))
    
    #print(df.dump())
    #print(len(df.chunks))
    #print(len(json.dumps(df.dump(),default=str)))
    
    df_hash = df._hash
    df.store()
    
    print('---------------------------------------------------------')
    df = DataFile(df_hash)
    print(df)
    
    for chunk in df.chunks:
        print('###################################################')
        print(json.dumps(chunk.dump(), indent=4, default=str))
    
    print(df.chunks[2].c.heart_rate)
    print(df.chunks[2].c.heart_rate.median)
    
    print(df.chunks[2].c.heart_rate.x)
    print(df.chunks[2].c.heart_rate.y)
    print(df.chunks[2].c.rr_int.x)
    print(df.chunks[2].c.rr_int.y)
