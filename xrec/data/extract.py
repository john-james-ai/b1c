#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ======================================================================================================================== #
# Project  : Explainable Recommendation (XRec)                                                                             #
# Version  : 0.1.0                                                                                                         #
# File     : \extract.py                                                                                                   #
# Language : Python 3.8                                                                                                    #
# ------------------------------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                                                    #
# Company  : Bryant St. Labs                                                                                               #
# Email    : john.james.ai.studio@gmail.com                                                                                #
# URL      : https://github.com/john-james-ai/xrec                                                                         #
# ------------------------------------------------------------------------------------------------------------------------ #
# Created  : Friday, December 10th 2021, 1:10:55 am                                                                        #
# Modified : Saturday, December 11th 2021, 12:22:23 pm                                                                     #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
import os
from datetime import datetime
from multiprocessing import Pool, Queue, current_process, freeze_support
import logging
from xrec.data.source import AmazonSource
# ------------------------------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------------------------------ #


def download_file(task: dict) -> dict:
    """Worker responsible for downloading file given a download task

    Args:
        task : Dictionary containing file metadata including url and local filepath.

    """
    start = datetime.now()
    with urllib.request.urlopen(task['url']) as response:
        f = open(task['filepath'], 'wb')
        f.write(response.read())
        f.close()
    end = datetime.now()
    duration = end - start
    task['downloaded'] = True
    task['download_date'] = np.datetime64(end)
    task['download_duration'] = duration
    task['download_size'] = os.path.getsize(task['filepath'])
    return task


def download_callback(task: dict) -> None:
    """Updates the AmazonSource metadata.

    Args:
        task : Dictionary containing file metadata including url and local filepath.

    """
    metadata = AmazonSource()
    metadata.update_metadata(key=task['key'],
                             kind=task['kind'],
                             downloaded=task['downloaded'],
                             download_date=task['download_date'],
                             download_duration=task['download_duration'],
                             download_size=task['download_size'])
