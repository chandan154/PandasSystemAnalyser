import os
import json
import csv
import requests
import time


def psaz(interval_nu, epoch):

    with open('psaz_map.txt', 'a', newline='') as file:
        file.writelines(f"{interval_nu}:{epoch}\n")

