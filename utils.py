import pandas as pd

def filter_df(df,col='DT', filter_dt='2022-07-24'):
    return df[df[col]<filter_dt]

def get_timeline(df, window_length='5',col='ORIGIN_TO_ADDRESS',method='count',x1='COUNTS',filter_dt='2022-07-23',hue=None):
    df = df.copy()
    df.index = pd.to_datetime(df['BLOCK_TIMESTAMP'])
    df.index.name = 'DT'
    ser = df.resample(f'{window_length}min')[col]
    ser = eval(f"ser.{method}()")
    ser =  ser.to_frame(x1).reset_index()
    return filter_df(ser,col='DT', filter_dt=filter_dt)
