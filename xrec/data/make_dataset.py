#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \make_dataset.py                                                                                              #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Your Company                                                                                                  #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/b1c                                                                          #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Saturday, December 4th 2021, 5:29:26 am                                                                       #
# Modified : Thursday, December 9th 2021, 6:03:44 pm                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Your Company                                                                                         #
# ======================================================================================================================== #
# %%
import requests
import logging
from bs4 import BeautifulSoup
from .utils.config import Config
# ------------------------------------------------------------------------------------------------------------------------ #


def get_urls(url):
    """Obtains the URLs for each Amazon rating and review file."""
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find('table', attrs={'class': 'code-table'})


def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    c = Config()
    url = c.read('DATA', 'url')
    urls = get_urls(url)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    main()
