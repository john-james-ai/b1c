#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \test_config.py                                                                                               #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Bryant St. Labs                                                                                               #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/xrec                                                                         #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Thursday, December 9th 2021, 1:59:45 pm                                                                       #
# Modified : Thursday, December 9th 2021, 5:02:58 pm                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
# %%
import pytest
import logging
import inspect
from xrec.utils.config import Config
# ------------------------------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigTests:

    def test_setup(self):
        section = 'DATA'
        key = 'url'
        value = 'https://nijianmo.github.io/amazon/index.html'
        c = Config()
        c.create(section, key, value)

    def test_exists(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()
        section, key = 'DATA', 'url'

        assert c.exists(section, key), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert not c.exists(section, "bogus"), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        assert c.exists(section), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))
        assert not c.exists("bogus"), \
            logger.error("     Failure in {}.".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        c = Config()

        section, key = 'DATA', 'url'
        value = c.read(section, key)
        assert isinstance(value, str), logger.error(
            "     Failure in {}".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_create(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        section, key, value = 'test', 'test_key', 'test_value'
        c = Config()
        c.create(section, key, value)

        value2 = c.read(section, key)
        assert value == value2, \
            logger.error("     Failure in {}".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        section, key, value = 'test', 'test_key', 'test_value_update'

        c = Config()
        c.update(section, key, value)

        value2 = c.read(section, key)
        assert value == value2, \
            logger.error("     Failure in {}".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete(self):
        logger.info("    Started {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

        # Delete Item
        section, key, value = 'test', 'test_key', 'test_value_update'

        c = Config()
        c.delete(section, key)
        assert not c.exists(section, key),\
            logger.error("     Failure in {}".format(inspect.stack()[0][3]))

        c.create(section, key, value)

        assert c.exists(section, key), \
            logger.error("     Failure in {}".format(inspect.stack()[0][3]))

        c.delete(section)
        assert not c.exists(section), \
            logger.error("     Failure in {}".format(inspect.stack()[0][3]))

        logger.info("    Successfully completed {} {}".format(
            self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        section = 'DATA'
        key = 'url'
        value = 'https://nijianmo.github.io/amazon/index.html'
        c = Config()
        c.create(section, key, value)


if __name__ == "__main__":
    t = ConfigTests()
    t.test_setup()
    t.test_exists()
    t.test_read()
    t.test_create()
    t.test_update()
    t.test_delete()


# %%
