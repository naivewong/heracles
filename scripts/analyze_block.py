import os
from os import listdir
from os.path import isfile, join
import sys

block = sys.argv[1]
# print(block)

dirs = [join(block, f) for f in listdir(block) if not isfile(join(block, f))]
chunks = []
indices = []
for d in dirs:
    cd = join(d, "chunks")
    chunks.extend([join(cd, f) for f in listdir(cd) if isfile(join(cd, f))])
    indices.append(join(d, "index"))

chunks_size = 0
indices_size = 0
for c in chunks:
    chunks_size += os.path.getsize(c)
for i in indices:
    indices_size += os.path.getsize(i)

print("chunks_size", chunks_size, "indices_size", indices_size, "total_size", chunks_size+indices_size)
