#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \test_data_source.py                                                                                          #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Bryant St. Labs                                                                                               #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/xrec                                                                         #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Thursday, December 9th 2021, 9:38:37 pm                                                                       #
# Modified : Friday, December 10th 2021, 3:26:33 pm                                                                        #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
# %%
import os
import pytest
import logging
import inspect
import pandas as pd
import numpy as np
from datetime import datetime
from xrec.data.source import AmazonSource
from xrec.utils.config import Config
# ------------------------------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AmazonSourceTests:

    def test_setup(self):
        logger.info("Started {}".format(self.__class__.__name__))

    def test_create_metadata(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        url = c.read('DATA', 'url')
        amazon = AmazonSource(url)
        amazon.create_metadata()
        assert isinstance(amazon.metadata, pd.DataFrame), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert amazon.metadata.shape[0] > 50, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert amazon.metadata.shape[1] == 11, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_metadata(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        url = c.read('DATA', 'url')
        amazon = AmazonSource(url)

        # Test without parameters
        metadata = amazon.read_metadata()
        print(metadata.info())
        assert isinstance(metadata, pd.DataFrame), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[0] > 50, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[1] == 11, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        # Test with key only
        metadata = amazon.read_metadata(key='books')
        assert isinstance(metadata, pd.DataFrame), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[0] == 2, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[1] == 11, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        # Test with key and kind
        metadata = amazon.read_metadata(key='books', kind='p')
        assert isinstance(metadata, pd.DataFrame), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[0] == 1, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert metadata.shape[1] == 11, \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_update_metadata(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        url = c.read('DATA', 'url')
        amazon = AmazonSource(url)
        key = 'video'
        kind = 'r'
        download_date = np.datetime64(datetime.now())
        download_duration = 2398
        download_size = 126543
        amazon.update_metadata(key, kind, download_date,
                               download_duration, download_size)
        result = amazon.read_metadata(key, kind)

        assert result['download_date'].values[0] == download_date,\
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert result['download_duration'].values[0] == download_duration,\
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert result['download_size'].values[0] == download_size,\
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_get_extract_tasks(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        url = c.read('DATA', 'url')
        amazon = AmazonSource(url)
        tasks = amazon.get_extract_tasks()

        assert isinstance(tasks, list), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        for task in tasks:
            assert isinstance(task, dict), \
                logger.error("     Failure in {}.".format(
                    inspect.stack()[0][3]))
            assert 'url' in task.keys(), \
                logger.error("     Failure in {}.".format(
                    inspect.stack()[0][3]))
            assert 'filepath' in task.keys(), \
                logger.error("     Failure in {}.".format(
                    inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete_metadata(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        url = c.read('DATA', 'url')
        amazon = AmazonSource(url)
        amazon.delete_metadata()

        assert amazon.metadata is None,\
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        logger.info("Successfully completed {}".format(
            self.__class__.__name__))


if __name__ == "__main__":
    t = AmazonSourceTests()
    t.test_setup()
    # t.test_create_metadata()
    t.test_read_metadata()
    t.test_update_metadata()
    t.test_get_extract_tasks()
    t.test_delete_metadata()
    t.test_teardown()


# %%