import os
import sys
import math
import time
import json
import glob
import logging
import argparse
import traceback
from pprint import pprint
from datetime import datetime as dt
from concurrent.futures import as_completed, ProcessPoolExecutor, ThreadPoolExecutor

import psutil
import requests
import numpy as np 
import pandas as pd
from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import *
from selenium.webdriver.support import expected_conditions as EC


def get_logger():
    log = logging.getLogger(os.path.split(__file__)[-1])
    log_level = logging.INFO
    log.setLevel(log_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(levelname)s: %(asctime)s - %(name)s:%(lineno)d - %(message)s')
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)
    return log

log = get_logger()
NA = "NA"


class HomesNZ:

    def __init__(self, settings):
        self.url = "https://homes.co.nz/"
        self.output = []
        self.settings = settings
        self.chrome = self.get_chrome_driver()

    def get_chrome_driver(self):
        chrome_options = webdriver.ChromeOptions()
        driver_path = self.settings["driver_path"]["value"]

        return webdriver.Chrome(driver_path, options=chrome_options)

    def type_in_search_bar(self, input_):
        try:
            search_bar = self.chrome.find_element_by_id("autocomplete-search")
            search_bar.clear()
            search_bar.send_keys(input_)
        except StaleElementReferenceException as _:
            time.sleep(1)
            self.type_in_search_bar(input_)

    def click_option_in_dropdown(self, ip_suburb, ip_region):

        dropdown = self.chrome.find_element_by_class_name("addressResults")
        dropdown_options = dropdown.find_elements_by_class_name("addressResult")

        def get_suburb(element):
            try:
                return element.find_element_by_class_name("addressResultStreet").text.strip()
            except NoSuchElementException as _:
                return ""

        def get_region(element):
            try:
                return element.find_element_by_class_name("addressResultSuburb").text.strip()
            except NoSuchElementException as _:
                return ""

        found, default = None, None
        options = []
        for option in dropdown_options:
            suburb, region = get_suburb(option), get_region(option)

            if region.lower() == "auckland":
                default = option

            if (suburb.lower() == ip_suburb.lower()) and (region.lower() == ip_region.lower()):
                found = option
            
            options.append(option)

        chosen = found if found else default
        if not chosen:
            return NA, NA

        chosen_suburb, chosen_region = get_suburb(chosen), get_region(chosen)
        chosen.click()
        
        return chosen_suburb, chosen_region

    def get(self, input_):
        try:
            wait = self.settings["page_load_timeout"]["value"]

            for ip in input_:
                ip_suburb, ip_region = ip["Suburb"], ip["Region"]

                try:
                    log.info(f"Fetching for {ip_suburb} - {ip_region}")

                    self.chrome.get(self.url)
                    WebDriverWait(self.chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "heroImage")))
                    WebDriverWait(self.chrome, wait).until(EC.presence_of_element_located((By.ID, "autocomplete-search")))

                    self.type_in_search_bar(ip_suburb)
                    WebDriverWait(self.chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "addressResults")))

                    chosen_suburb, chosen_region = self.click_option_in_dropdown(ip_suburb, ip_region)
                    if chosen_suburb == NA and chosen_region == NA:
                        log.info(f"Skipped {ip_suburb} - {ip_region} ...")
                        self.output.append({
                            "suburb": ip_suburb,
                            "region": ip_region,
                            "median_estimate": NA,
                            "period1": NA,
                            "capital_growth": NA,
                            "period2": NA,
                            "chosen_area": f"{chosen_suburb} - {chosen_region}"
                        })
                    else:
                        WebDriverWait(self.chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "statValue")))
                        self.output.append({
                            "suburb": ip_suburb,
                            "region": ip_region,
                            "median_estimate": self.chrome.find_elements_by_class_name("statValue")[0].text.strip(),
                            "period1": self.chrome.find_elements_by_class_name("statNote")[0].text.strip(),
                            "capital_growth": self.chrome.find_elements_by_class_name("statValue")[-1].text.strip(),
                            "period2": self.chrome.find_elements_by_class_name("statNote")[-1].text.strip(),
                            "chosen_area": f"{chosen_suburb} - {chosen_region}"
                        })

                except TimeoutException as _:
                    log.error(f"NOT FOUND. Couldn't fetch data for {ip_suburb} - {ip_region}")

        except Exception as _:
            traceback.print_exc()
        finally:
            self.shutdown()
            return self.output

    def shutdown(self):
        process = psutil.Process(self.chrome.service.process.pid)
        for child_process in process.children(recursive=True):
            try:
                if psutil.pid_exists(child_process.pid):
                    log.debug(f"Killing child process: ({child_process.pid}) - {child_process.name()} [{child_process.status()}]")
                    child_process.kill()
            except Exception as _:
                log.debug(f"Couldn't kill process ({child_process.pid}). May be already killed!")
        
        log.debug(f"Killing main process: ({process.pid}) - {process.name()} [{process.status()}]")
        process.kill()
        self.chrome.quit()


def get(input_):
    homes = HomesNZ(get_settings())
    return homes.get(input_)


def run_concurrent(settings):
    inputs = pd.read_excel("input.xlsx").to_dict("records")
    workers = settings["workers"]["value"]
    chunk_inputs = list(map(lambda x: list(x), np.array_split(inputs, workers)))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        datas = []
        
        for data in executor.map(get, chunk_inputs):
            if data:
                datas.extend(data)

    return datas


def sanitize_data(data):
    return data


def get_settings():
    with open("settings.json", "r") as f:
        return json.load(f)


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    settings = get_settings()
    data = run_concurrent(settings) or []
    data = sanitize_data(data)
    df = pd.DataFrame(data)
    df.to_excel("output.xlsx", index=False, engine="xlsxwriter")

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
