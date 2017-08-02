def get_frequentation_diff():
    """
    difference in station frequentation overused / not-overused bikes
    :return: frequentation difference per station in percent
    """
    data = get_data()
    trip_duration_by_bike = get_tripduration_by_bike()
    threshold = get_threshold_outlier()

    # Outliers:
    outlier_bikes = trip_duration_by_bike[trip_duration_by_bike['tripduration'] > threshold]
    outliers_bike_ids = np.array(outlier_bikes[['bikeid']]).flatten()
    data_outliers_bike = data[data['bikeid'].isin(outliers_bike_ids)]
    data_outliers_bike['freq'] = data_outliers_bike.groupby('start station id')['start station id'].transform('count')
    start_station_outliers = data_outliers_bike[['start station id', 'freq']]\
        .drop_duplicates().sort_values(by='freq', ascending=False)
    start_station_outliers['freq'] = [round(1000 * fr / sum(start_station_outliers['freq']), 2) for fr in
                                      start_station_outliers['freq']]

    # Not outliers:
    not_outlier_bikes = trip_duration_by_bike[trip_duration_by_bike['tripduration'] < threshold]
    not_outliers_bike_ids = np.array(not_outlier_bikes[['bikeid']]).flatten()

    data_not_outliers_bike = data[data['bikeid'].isin(not_outliers_bike_ids)]
    data_not_outliers_bike['freq'] = data_not_outliers_bike.groupby('start station id')['start station id'].transform(
        'count')
    start_station_not_outliers = data_not_outliers_bike[['start station id', 'freq']].drop_duplicates().sort_values(
        by='freq', ascending=False)
    start_station_not_outliers['freq'] = [round(1000 * fr / sum(start_station_not_outliers['freq']), 2) for fr in
                                          start_station_not_outliers['freq']]

    # Comparison:
    comparison_outliers = start_station_outliers.merge(start_station_not_outliers, on='start station id')
    comparison_outliers.columns = ['start station id', 'permille_not_outlier', 'permille_outlier']
    comparison_outliers['abs_diff_percent'] = comparison_outliers \
        .apply(lambda row: abs(percent_difference(row['permille_outlier'], row['permille_not_outlier'])), axis=1)
    comparison_outliers['diff_percent'] = comparison_outliers \
        .apply(lambda row: percent_difference(row['permille_outlier'], row['permille_not_outlier']), axis=1)
    comparison_outliers = comparison_outliers.sort_values(by='abs_diff_percent', ascending=False)

    return comparison_outliers


def cartesian(df1, df2):
    rows = itertools.product(df1.iterrows(), df2.iterrows())

    df = pd.DataFrame(left.append(right) for (_, left), (_, right) in rows)
    return df.reset_index(drop=True)

def get_all(new=False):
    """
    :param new: recompute or not
    :return: all dates by hours in the time span of data frame
    """
    if new == False and os.path.exists('all.pkl'):
        return pd.read_pickle('all.pkl')

    all_days = pd.DataFrame({'day': pd.date_range('02.01.2016', periods=29, freq='d')})
    all_days['day'] = all_days['day'].apply(lambda x: pd.Timestamp(x))
    all_days['day'] = all_days['day'].dt.strftime('%m.%d.%Y')
    all_hours = pd.DataFrame({'hour': pd.date_range('02.01.2016', periods=24, freq='H')})
    all_hours['hour'] = all_hours['hour'].apply(lambda x: pd.Timestamp(x))
    all_hours['hour'] = all_hours['hour'].dt.strftime('%H')
    all_dates = cartesian(all_days, all_hours)

    all_stations = get_stations()[['id']] #watch out: global stations
    all_stations.columns = ['station id']

    all_ = cartesian(all_stations, all_dates)
    all_.to_pickle('all.pkl')
    return all_

def get_fdata(new=False):
    """
    :param new: recompute or not
    :return: data with desired features
    """
    if new == False and os.path.exists('fdata.pkl'):
        return pd.read_pickle('fdata.pkl')

    data = get_data()
    features = ['starttime', 'stoptime', 'start station id', 'start_closest', 'end station id', 'end_closest']
    fdata = data[features]
    tstart = time.time()
    print 'stime:'
    fdata['startday'] = fdata['starttime'].dt.strftime('%m.%d.%Y')
    fdata['starthour'] = fdata['starttime'].dt.strftime('%H')
    fdata['stopday'] = fdata['stoptime'].dt.strftime('%m.%d.%Y')
    fdata['stophour'] = fdata['stoptime'].dt.strftime('%H')
    fdata.to_pickle('fdata.pkl')
    return fdata

def get_unbalance(fdata):
    """
    :param fdata: data with desired features
    :return: nbalance by station, day and hour
    """
    fdata['one'] = [1.0]*fdata.shape[0]

    all_ = get_all()