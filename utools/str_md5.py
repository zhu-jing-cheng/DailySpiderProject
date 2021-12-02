# -*- coding: utf-8 -*-
import sys
sys.path.append("../../..")
import hashlib


def md5Encode(data):
    m = hashlib.md5(str(data).encode("utf-8")).hexdigest()
    return m