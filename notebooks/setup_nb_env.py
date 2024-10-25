# from IPython.core.display import display, HTML
# display(HTML("<style>.container { width:100% !important; }</style>"))
from IPython.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
import sys
sys.path.insert(0, '..')
sys.path.insert(0, '../epsampling/')

import random
import numpy as np
import pandas as pd
import os
import csv
import sys
from tqdm import tqdm
 
import warnings
warnings.filterwarnings("ignore")
np.set_printoptions(suppress=True,formatter={'float_kind': '{:f}'.format})

# from IPython.display import Audio, display
# def allDone():
#     urL = 'https://wavlist.com/wav/cat-meow4.wav'
#     display(Audio(url=urL, autoplay=True))
    

from IPython.display import Audio
def allDone():
    display(Audio(filename='../cat_meow2.wav', autoplay=True))

import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('max_colwidth', None)

# from salsa.chef import load_model
# from salsa.chef import serve_salsa
# from salsa.datasets import convert_vec_to_smi
# from salsa.utils import get_cansmiles
# from rdkit.Chem import AllChem

from datetime import datetime