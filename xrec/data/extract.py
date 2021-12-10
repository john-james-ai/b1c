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
# Modified : Friday, December 10th 2021, 12:58:25 pm                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                                                   #
# ------------------------------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                                                       #
# Copyright: (c) 2021 Bryant St. Labs                                                                                      #
# ======================================================================================================================== #
from datetime import datetime
from multiprocessing import Pool, Queue, current_process, freeze_support
import logging
from xrec.data.source import AmazonSource
# ------------------------------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------------------------------ #


def download_file(task) -> None:
    """Worker responsible for downloading file given a download task

    Arguments:
        task : Dictionary containing file metadata including url and local filepath.

    """
    start = datetime.now()
    with urllib.request.urlopen(task['url']) as response:
        f = open(task['filepath'], 'wb')
        f.write(response.read())
        f.close()
