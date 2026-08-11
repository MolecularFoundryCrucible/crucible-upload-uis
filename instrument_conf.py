import os
HOME = os.environ.get('HOME')

DEFAULT_BROWSE_DIR = '/home/timko/Documents'
# True  = pick a folder; create parent session dataset + one child dataset per file inside
# False = pick one or more files; each becomes its own standalone dataset (or insitu, per instrument)
IS_SESSION = False
DEFAULT_INSTRUMENT_NAME = 'nirvana'
DEFAULT_INGESTOR = ''

# True  = run an instrument's post-processing requests sequentially; each depends on
#         the previous succeeding (a failure halts the rest).
# False = request all of them in parallel (independent of each other).
CHAIN_POST_PROCESSING = True
DRY_RUN = True

PRINT_BARCODE_ENABLED = False
ACCEPTABLE_FILE_TYPES = {'.bcf', '.bmp', '.csv', '.dm3', '.dm4', '.emd', '.h5', '.jpeg', '.mcr', '.mtd', '.png', '.ser', '.tif', '.txt', '.xls'}

'''
To enable barcode printing:
- set PRINT_BARCODE_ENABLED to True
- Connect a brother pt-d610bt label printer to the computer running this code, and set the printer name in the print_label function in backend.py
- Install the required libraries: uv add pywin32
- Install printer driver from here: https://support.brother.com/g/b/downloadtop.aspx?c=us&lang=en&prod=d610bteus
- Find the printer in settings under printers and scanners and note the exact name (e.g. "Brother PT-D610BT")
- Download the brothers SDK for Windows B-pac (made a free account)
- Set printer settings through windows to match the tape type and size that you want to print (https://docs.google.com/presentation/d/1vSS1Xp0fzIwflpj50vx5LOO9MuW7FtZhLS1EQ7D4opI/edit?usp=sharing)
'''
