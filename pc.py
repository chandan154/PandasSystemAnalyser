import os
import pandas as pd
import json
from psaz import psaz
import sys
import csv
import requests
import time
from configparser import ConfigParser


def psaz_collect(filename):
    import shutil
    j = 0
    k = 0
    parser = ConfigParser()
    parser.read(filename)
    data_directory_isize = parser.getint('PSAZ', 'data_directory_isize')
    data_interval = parser.getint('PSAZ', 'data_interval')
    main_dir = parser.get('PSAZ', 'data_dir')
    data_retention = parser.getint('PSAZ', 'data_retention')
    try:
        if os.path.getsize('psaz_map.txt') != 0 and os.path.isfile('psaz_map.txt'):
            actual_k = int(start_from_last())  #3
            k = actual_k + 1               #4
            j = 0  #1
    except FileNotFoundError:
        pass

    if k - 1 >= data_retention:
        try:
            shutil.rmtree(f"{main_dir}/psaz_data.{(k - 1) - data_retention}")
        # os.rmdir(f"{main_dir}/psaz_data.{(k - 1) - data_retention}")
        except FileNotFoundError:
            pass
    specific_data_list = ['cpu', 'percpu', 'mem', 'memswap', 'processcount','processlist', 'load', 'diskio', 'fs', 'sensors']

    while True:
        if j % data_directory_isize == 0:
            sub_dir = f"psaz_data.{k}"
            path = os.path.join(main_dir, sub_dir)
            os.mkdir(path)
            psaz(k, int(time.time()))

            for specific_data in specific_data_list:
                f = open(f"{main_dir}/psaz_data.{k}/{specific_data}.csv", 'a', newline='')


            k = k + 1

        if k >= data_retention+1:
            try:
                shutil.rmtree(f"{main_dir}/psaz_data.{(k - (data_retention + 1))}")
                # os.rmdir(f"{main_dir}/psaz_data.{k - data_retention}")
            except FileNotFoundError:
                pass

        j = j + 1

        for specific_data in specific_data_list:
            path_of_csv_file = f"{main_dir}/psaz_data.{k - 1}/{specific_data}.csv"
            data_file = open(f"{main_dir}/psaz_data.{k - 1}/{specific_data}.csv", 'a', newline='')

            csv_reader = csv.reader(data_file)

            a = requests.get("http://localhost:61208/api/3/{specific_data}".format(specific_data=specific_data)).json()

            if specific_data != 'processlist':

                if type(a) == dict:
                    # a['timestamp'] = int(time.time())
                    # if os.path.getsize(path_of_csv_file) == 0:
                    #
                    #     header = a.keys()
                    #     csv_writer.writerow(header)
                    #
                    # csv_writer.writerow(a.values())
                    # data_file.close()
                    a = [a]

                for item in a:
                    item["timestamp"] = int(time.time())

                csv_writer = csv.DictWriter(data_file, fieldnames=a[0].keys(), extrasaction="ignore")

                if os.path.getsize(path_of_csv_file) == 0:

                    csv_writer.writeheader()
                csv_writer.writerows(a)


                # if type(a) == list:
                #         # a[0]['timestamp'] = int(time.time())
                #     for i in range(len(a)):
                #         if os.path.getsize(path_of_csv_file) == 0:
                #             header = a[0].keys()
                #             csv_writer.writerow(header)
                #         csv_writer.writerow(a[i].values())
                #     data_file.close()
            else:
                processlist_flatten(a, data_file)

        time.sleep(data_interval)





# def main():
#     filename = sys.argv[1]
#     psaz_collect(filename)
#
#
# if __name__ == "__main__":
#     main()


def start_from_last():
    if os.path.isfile('psaz_map.txt'):
        with open("psaz_map.txt", 'r', newline='') as map_file:
            last_dir = map_file.readlines()[-1]
            last_dir_sliced = last_dir.split(':')
            last_dir_made = last_dir_sliced[0]
            last_dir_sliced[1] = last_dir_sliced[1].replace("\n", "")

    return last_dir_made


def last_j_count():
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read('psaz.conf')
    main_dir = parser.get('data', 'data_dir')
    j_count = 0
    last_dir_made = start_from_last()
    with open(f"{main_dir}/psaz_data.{last_dir_made}/cpu.csv") as cpu_csv_file:
        for rows in cpu_csv_file:
            j_count += 1
    return j_count - 1


def processlist_flatten(a, data_file):
    df_a = pd.DataFrame(a)
    df_a['epoch'] = int(time.time())
    for row in df_a.loc[df_a.memory_info.isnull(), 'memory_info'].index:
        df_a.at[row, 'memory_info'] = [0, 0, 0, 0]
    for row in df_a.loc[df_a.cpu_times.isnull(), 'cpu_times'].index:
        df_a.at[row, 'cpu_times'] = [0, 0, 0, 0]
    memory_info = pd.DataFrame(df_a['memory_info'].to_list(), columns=['rss', 'vms', 'shared', 'text'])
    cpu_times = pd.DataFrame(df_a['cpu_times'].to_list(), columns=['parent_user_time', 'parent_system_time',
                                                                   'child_user_time', 'child_system_time'])
    io_counters = pd.DataFrame(df_a['io_counters'].to_list(),
                               columns=['io_counter0', 'io_counter1', 'io_counter2', 'io_counter3', 'io_counter4'])
    df_a['readio'] = io_counters['io_counter0'] - io_counters['io_counter2']
    df_a['writeio'] = io_counters['io_counter1'] - io_counters['io_counter3']
    df_a = pd.concat([df_a, memory_info, cpu_times], axis=1)
    df_a = df_a.drop(['memory_info'], axis=1)
    df_a = df_a.drop(['cpu_times'], axis=1)
    df_a = df_a.drop(['io_counters'], axis=1)
    df_a.to_csv(data_file, index=False)





psaz_collect(filename=sys.argv[1])


