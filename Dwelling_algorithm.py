def DwellinG(dat):
    # Merge block
    winSpec = Window.partitionBy('cell_id', 'imsi').orderBy('TIME')
    dat1 = dat.withColumn('S_E', f.lit(+1)).withColumnRenamed('start_time', 'TIME').select('cell_id', 'imsi', 'TIME',
                                                                                           'S_E')
    unidat = dat1.union(
        dat.withColumn('S_E', f.lit(-1)).withColumnRenamed('end_time', 'TIME').select('cell_id', 'imsi', 'TIME', 'S_E'))
    unidat = (unidat
              .withColumn('OVERLAP_COUNT', f.sum('S_E').over(winSpec))
              .withColumn('SECS_SINCE_LAST',
                          f.coalesce(
                              (0 == f.lag('OVERLAP_COUNT').over(winSpec)).cast('int') * (
                                          f.unix_timestamp('TIME') - f.lag(f.unix_timestamp('TIME')).over(winSpec)),
                              f.lit(0))
                          )
              .withColumn('DETACH_PREV', (~(f.col('SECS_SINCE_LAST') < 3600)).cast('int'))
              .withColumn('GROUP_ID', f.sum(f.coalesce(f.col('DETACH_PREV'), f.lit(0))).over(winSpec))
              .groupBy(['cell_id', 'imsi', 'GROUP_ID'])
              .agg(
        f.min(f.col('TIME')).alias('START'),
        f.max(f.col('TIME')).alias('END'),
        (f.count('*') / 2).alias('ROWS_SB'))
              .withColumn('DWELL_TIME_MINUTES',
                          ((f.unix_timestamp(f.col('END')) - f.unix_timestamp(f.col('START'))) / 60).cast('int'))
              .select('cell_id', 'imsi', 'START', 'END', 'DWELL_TIME_MINUTES', 'ROWS_SB')
              )

    # Overlap Block
    winSpec = Window.partitionBy('imsi').orderBy('TIME')

    dat1 = (unidat
            .withColumn('S_E', f.lit(+1))
            .withColumn('TIME', f.col('START'))
            .select('cell_id', 'imsi', 'TIME', 'S_E', 'START', 'END', 'DWELL_TIME_MINUTES', 'ROWS_SB')
            )
    dat2 = (unidat
            .withColumn('S_E', f.lit(-1))
            .withColumn('TIME', f.col('END'))
            .select('cell_id', 'imsi', 'TIME', 'S_E', 'START', 'END', 'DWELL_TIME_MINUTES', 'ROWS_SB')
            )

    findat = dat1.union(dat2)

    interv = (findat
              .withColumn('OVERLAP_COUNT', f.coalesce(f.sum('S_E').over(winSpec), f.lit(0)))
              .withColumn('time_section', f.lead('TIME').over(Window.partitionBy('imsi').orderBy('TIME')))
              .filter(f.col('OVERLAP_COUNT') > 0)
              .withColumn('group', f.rank().over(Window.partitionBy('imsi').orderBy('TIME')))
              .select('imsi', 'TIME', 'time_section', 'group')
              .withColumnRenamed('imsi', 'imsi1')
              )
    jtemp = (interv
             .join(unidat,
                   [(interv.imsi1 == unidat.imsi), (unidat.START <= interv.TIME), (unidat.END >= interv.time_section)],
                   'right')
             .drop('imsi1')
             )
    max_obs = (jtemp
               .withColumn('max', f.when(
        f.col('DWELL_TIME_MINUTES') == f.max('DWELL_TIME_MINUTES').over(Window.partitionBy('imsi', 'group')),
        1).otherwise(0))
               .withColumn('max', f.when(f.isnull(f.col('group')), 1).otherwise(f.col('max')))
               .filter(f.col('max') == 1)
               .select('imsi', 'TIME', 'time_section', 'cell_id', 'imsi')
               .withColumnRenamed('TIME', 'START')
               .withColumnRenamed('time_section', 'END')
               )

    # Merge again
    winSpec = Window.partitionBy('cell_id', 'imsi').orderBy('TIME')
    dat1 = max_obs.withColumn('S_E', f.lit(+1)).withColumnRenamed('START', 'TIME').select('cell_id', 'imsi', 'TIME',
                                                                                          'S_E')
    Nunidat = dat1.union(
        max_obs.withColumn('S_E', f.lit(-1)).withColumnRenamed('END', 'TIME').select('cell_id', 'imsi', 'TIME', 'S_E'))
    Nunidat = (Nunidat
               .withColumn('OVERLAP_COUNT', f.sum('S_E').over(winSpec))
               .withColumn('SECS_SINCE_LAST',
                           f.coalesce(
                               (0 == f.lag('OVERLAP_COUNT').over(winSpec)).cast('int') * (
                                           f.unix_timestamp('TIME') - f.lag(f.unix_timestamp('TIME')).over(winSpec)),
                               f.lit(0))
                           )
               .withColumn('DETACH_PREV', (~(f.col('SECS_SINCE_LAST') < 3600)).cast('int'))
               .withColumn('GROUP_ID', f.sum(f.coalesce(f.col('DETACH_PREV'), f.lit(0))).over(winSpec))
               .groupBy(['cell_id', 'imsi', 'GROUP_ID'])
               .agg(
        f.min(f.col('TIME')).alias('START'),
        f.max(f.col('TIME')).alias('END'),
        (f.count('*') / 2).alias('ROWS_SB'))
               .withColumn('DWELL_TIME_MINUTES',
                           ((f.unix_timestamp(f.col('END')) - f.unix_timestamp(f.col('START'))) / 60).cast('int'))
               .select('cell_id', 'imsi', 'START', 'END', 'DWELL_TIME_MINUTES', 'ROWS_SB')
               )
    return Nunidat