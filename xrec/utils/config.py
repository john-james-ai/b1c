#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \config.py                                                                                                    #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Bryant St. Labs                                                                                               #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/xrec                                                                         #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Thursday, December 9th 2021, 1:33:39 pm                                                                       #
# Modified : Thursday, December 9th 2021, 5:02:52 pm                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
import configparser
from typing import Union
import logging
# ------------------------------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


class Config:
    """Manages project configuration"""
    configfile = "config/config.ini"

    def create(self, section, key, value) -> None:
        """Creates a configuration key/value pair to a section."""
        config = configparser.ConfigParser()
        config.read(Config.configfile)

        try:
            config[section][key] = value
        except KeyError:
            config[section] = {}
            config[section][key] = value
        except Exception as e:
            logger.error(e)

        with open(Config.configfile, 'w') as configfile:
            config.write(configfile)

    def read(self, section, key) -> Union[dict, str]:
        """Reads a section and returns a dictionary."""
        config = configparser.ConfigParser()
        config.read(Config.configfile)

        try:
            return config[section][key]
        except KeyError as k:
            logger.error(k)

    def update(self, section, key, value) -> None:
        """Updates the configuration section key with new value."""
        config = configparser.ConfigParser()
        config.read(Config.configfile)

        try:
            config[section][key] = value
            with open(Config.configfile, 'w') as configfile:
                config.write(configfile)
        except Exception as e:
            logger.error(e)

    def delete(self, section, key=None) -> None:
        """Deletes a key/value pair from config."""
        config = configparser.ConfigParser()
        config.read(Config.configfile)

        try:
            if key is not None:
                config.remove_option(section, key)
            else:
                config.remove_section(section)

            with open(Config.configfile, 'w') as configfile:
                config.write(configfile)
        except Exception as e:
            logger.error(e)

    def exists(self, section, key=None) -> bool:
        """Returns true if the section or item exists, false otherwise."""
        config = configparser.ConfigParser()
        config.read(Config.configfile)

        if key is None:
            return config.has_section(section)
        else:
            return config.has_option(section, key)
