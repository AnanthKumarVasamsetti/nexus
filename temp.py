import math
from pprint import pprint

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield (l[i:i + n][0], l[i:i + n][-1]+1)

def start():
    blocks = list(chunks(range(0,50965), math.floor(50965/4)))
    pprint(blocks)
start()