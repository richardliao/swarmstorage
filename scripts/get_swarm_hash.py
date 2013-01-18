#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:         get_swarm_hash.py
# Description:  
# Copyright:    Copyright (c) 2012
#-------------------------------------------------------------------------------

import sys
import os

import hashlib

chunk_size = 4 # 4<<20

def gen_sha1(source_str):
    """Generate sha1 key
    """
    return hashlib.sha1(source_str).hexdigest()

def get_swarm_hash(file_path):
    """计算文件的swarm_hash
    """
    hashes = []

    f = open(file_path, "rb")
    while True:
        chunk = f.read(chunk_size)
        if len(chunk)==0:
            break
        hashes.append(gen_sha1(chunk))
        if len(chunk) != chunk_size:
            f.close()
            break

    hash_list = "".join(hashes)
    swarm_hash = gen_sha1(hash_list)
    return swarm_hash

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usages: python %s file_path" % sys.argv[0]
        sys.exit(1)

    file_path = sys.argv[1]
    swarm_hash = get_swarm_hash(file_path)
    print swarm_hash

