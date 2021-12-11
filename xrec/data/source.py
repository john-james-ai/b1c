#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \web.py                                                                                                       #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Bryant St. Labs                                                                                               #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/xrec                                                                         #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Thursday, December 9th 2021, 5:27:54 pm                                                                       #
# Modified : Saturday, December 11th 2021, 3:44:16 pm                                                                      #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json
from email.utils import parsedate_to_datetime
from xrec.utils.config import Config

# ------------------------------------------------------------------------------------------------------------------------ #


class AmazonSource:
    """Manages the metadata for the Amazon reviews data sets.

    This class serves two purposes:
        1. Manage the metadata for the Amazon reviews data source
        2. Serve tasks to download the source data to workers operating in a multiprocessing environment.

    The interface includes:
        create_metadata: Extracts file metadata from the source site
        read_metadata: Returns metadata dataframe.
        update_metadata: Method called by the download callback. Updates metadata with download state and statistics.
        delete_metadata: Purges metadata
        get_keys = Returns a list of metadata keys
        describe: Describes a file
        get_extract_tasks: Serves data extraction tasks. Method called by the extract process.

    Attributes:
        metadata: DataFrame containing all metadata.

    Dependencies:
        Config: Class responsible for managing configuration information. Used to obtain the URL for the Amazon reviews data source.

    """

    def __init__(self) -> None:
        """Initializes the class with filepaths for metadata and data. """
        self.metadata = None
        self._config = Config()
        self._filepath_metadata = self._config.read(
            'DATA', 'amazon_metadata_uri')
        self._filepath_data = self._config.read(
            'DATA', 'data_external_amazon')

    def create_metadata(self, url: str):
        """Extracts and saves the metadata for the Amazon reviews and products data sets.

        Args:
            url: The URL for the Amazon reviews data source
        """
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        self.metadata = self._parse_table(soup)
        self._save()

    def read_metadata(self, key: str = None, kind: str = None) -> pd.DataFrame:
        """Returns metadata based upon selection criteria parameters

        Args:
            key: One-word key for the product category
            kind: Either 'r' for reviews or 'p'for products

        Returns:
            DataFrame (or Series) containing metadata for Amazon reviews or product files.
        """
        df = None
        self._check_metadata()
        if kind:
            kind = self._get_kind(kind)
        if key and kind:
            df = self.metadata[(self.metadata['key'] == key)
                               & (self.metadata['kind'] == kind)]
        elif key:
            df = self.metadata[self.metadata['key'] == key]
        elif kind:
            df = self.metadata[self.metadata['kind'] == kind]
        else:
            df = self.metadata
        return df

    def update_metadata(self, key: str, kind: str, downloaded: bool,
                        download_date: datetime, download_duration: int, download_size: int) -> None:
        """Updates the download metadata for an Amazon review or product file.

        Args:
            key: Key designating the product category
            kind: Either 'r' for reviews or 'p' for products.
            downloaded: True if file was downloaded. False otherwise.
            download_date: The datetime of the download.
            download_duration: The duration of the download in seconds.
            download_size: The number of bytes downloaded.

        """
        self._check_metadata()
        kind = self._get_kind(kind)
        cond = ((self.metadata['key'] == key) &
                (self.metadata['kind'] == kind))
        self.metadata.loc[cond, 'downloaded'] = downloaded
        self.metadata.loc[cond, 'download_date'] = download_date
        self.metadata.loc[cond, 'download_duration'] = download_duration
        self.metadata.loc[cond, 'download_size'] = download_size
        self._save()
        self._load()

    def delete_metadata(self) -> None:
        """Deletes metadata."""
        confirm = input(
            "Are you sure you wish to delete the Amazon metadata? [y/n]")
        if 'y' in confirm:
            self.metadata = None
            os.remove(self._filepath_metadata)

    def get_keys(self) -> list:
        """Returns a list of unique keys for the datasets."""
        self._check_metadata()
        return self.metadata['key'].unique()

    def describe(self, key: str, kind: str) -> str:
        """Describes the dataset for the specified key and kind.

        Args:
            key: The key for the dataset category
            kind: r for reviews or p for products
        Returns:
            string describing the file and its state.

        """
        self._check_metadata()
        kind = self._get_kind(kind)
        data = self.metadata[(self.metadata['key'] == key)
                             & (self.metadata['kind'] == kind)].to_dict(orient='records')[0]

        downloaded = ""
        if data['downloaded']:
            downloaded = " Downloaded on {} at {}".format(
                data['download_date'].date(),
                data['download_date'].time())
        description = data['category'] + " containing " + \
            str(data['n']) + ' ' + data['kind'] + ' in ' + \
            str(round(data['size'] / 1048576, 2)) + " Mb" + downloaded + '.'
        return description

    def reset_extract(self) -> None:
        """Resets extract metadata for all data sets."""
        self._check_metadata()
        if self.metadata is not None:
            self.metadata['downloaded'] = False
            self.metadata['download_date'] = np.datetime64(
                datetime.fromisoformat('1970-01-01'))
            self.metadata['download_duration'] = 0
            self.metadata['download_size'] = 0
            self._save()
            self._load()

    def get_extract_tasks(self, keys: list = [], max_tasks=100) -> list:
        """Returns a list of dictionaries containing metadata for files to be extracted.

        This method returns a list of dictionaries of max length = max_tasks. Each dictionary contains a key, kind (review or product), url and filepath. Each dictionary entry or task is assigned to a download worker for downloading. Only files that have not yet been downloaded are returned. If there are no remaining files to be downloaded, an empty list is returned.

        Args:
            keys: Optional list of keys for the files to be extracted. Default is None
                which would return data for files not yet downloaded.
            max_tasks: Maximum number of tasks to return.

        Returns:
            list of dictionaries containing data set metadata.

        """
        self._check_metadata()
        if len(keys) > 0:
            tasks = self.metadata[(self.metadata['key'].isin(
                keys)) & (~self.metadata['downloaded'])]
        else:
            tasks = self.metadata[~self.metadata['downloaded']]
        return tasks[['key', 'kind', 'url', 'filepath']].head(max_tasks).to_dict('records')

    def _parse_table(self, soup) -> pd.DataFrame:
        """Parses HTML table and returns metadata as list of dictionaries.

        Args:
            soup: Beautiful soup object containing web page information

        Returns:
            DataFrame containing all Amazon review and product file metadata.

        """
        metadata = []
        table = soup.find('table', attrs={'class': 'code-table'})
        rows = table.find_all('tr')
        for row in rows:
            reviews, products = self._parse_row(row)
            metadata.append(reviews)
            metadata.append(products)

        df = pd.DataFrame.from_records(metadata)
        return df

    def _parse_row(self, row) -> dict:
        """Extracts the data from a row on the HTML table on the source site."""

        tds = row.find_all('td')
        # Grab the text category and create a one-word key for the metadata dictionary
        key = self._extract_key(tds[0].text)
        category = tds[0].text

        # Get links
        links = row.find_all('a')

        # Format review links and metadata
        reviews_url = links[0].get('href')
        reviews_filepath = os.path.join(
            self._filepath_data, 'reviews' + '/', key + '.json.gz')
        reviews_num = int(tds[1].text.split()[1].replace(
            ',', '').replace('(', '').replace(')', ''))
        reviews_size = requests.get(
            reviews_url, stream=True).headers['Content-length']
        reviews_modified = parsedate_to_datetime(requests.get(
            reviews_url, stream=True).headers['last-modified'])
        reviews_download_date = np.datetime64(
            datetime.fromisoformat('1970-01-01'))
        reviews_download_duration = 0
        reviews_download_size = 0

        # And for products
        products_url = links[1].get('href')
        products_filepath = os.path.join(
            self._filepath_data, 'products' + '/', key + '.json.gz')
        products_num = int(tds[2].text.split()[1].replace(
            ',', '').replace('(', '').replace(')', ''))
        products_size = requests.get(
            products_url, stream=True).headers['Content-length']
        products_modified = parsedate_to_datetime(requests.get(
            products_url, stream=True).headers['last-modified'])
        products_download_date = np.datetime64(
            datetime.fromisoformat('1970-01-01'))
        products_download_duration = 0
        products_download_size = 0

        reviews = {'key': key,
                   'category': category,
                   'kind': 'reviews',
                   'n': reviews_num,
                   'size': reviews_size,
                   'modified': reviews_modified,
                   'downloaded': False,
                   'download_date': reviews_download_date,
                   'download_duration': reviews_download_duration,
                   'download_size': reviews_download_size,
                   'url': reviews_url,
                   'filepath': reviews_filepath}

        products = {'key': key,
                    'category': category,
                    'kind': 'products',
                    'n': products_num,
                    'size': products_size,
                    'modified': products_modified,
                    'downloaded': False,
                    'download_date': products_download_date,
                    'download_duration': products_download_duration,
                    'download_size': products_download_size,
                    'url': products_url,
                    'filepath': products_filepath}

        return reviews, products

    def _extract_key(self, s) -> str:
        """Extracts and creates a one-word key for the url dictionary entry."""
        s = s.replace(" and", "")
        s = s.replace("All ", "")
        s = s.replace("AMAZON ", "")
        s = s.replace("Digital ", "")
        s = s.replace("Gift ", "")
        s = s.replace("Musical ", "")
        s = s.replace("Prime ", "")
        s = s.strip().split()[0].lower()
        return s

    def _check_metadata(self) -> None:
        """Checks if metadata has been extracted, and if not extracts it."""
        if os.path.isfile(self._filepath_metadata):
            self._load()
        else:
            self.create_metadata()

    def _load(self) -> None:
        if os.path.isfile(self._filepath_metadata):
            self.metadata = pd.read_csv(
                self._filepath_metadata, index_col=False, parse_dates=['modified', 'download_date'])

    def _save(self) -> None:
        self.metadata.to_csv(self._filepath_metadata, header=True, index=False)

    def _get_kind(self, kind) -> str:
        """Converts kind parameter to a word because I'm that anal."""
        return 'products' if 'p' in kind else 'reviews'

    @property
    def n_files(self) -> int:
        return len(self.metadata.index)

    @property
    def n_files_downloaded(self) -> int:
        self._check_metadata()
        return (self.metadata['downloaded']).sum()
