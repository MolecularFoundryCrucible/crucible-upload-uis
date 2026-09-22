import os
HOME = os.environ.get('HOME')

DEFAULT_BROWSE_DIR = '/Users/mkwall/Documents/Test Data Folders'
# True  = pick a folder; create parent session dataset + one child dataset per file inside
# False = pick one or more files; each becomes its own standalone dataset (or insitu, per instrument)
IS_SESSION = False
DEFAULT_INSTRUMENT_NAME = 'gc-b30-001'
DEFAULT_INGESTOR = ''

# True  = run an instrument's post-processing requests sequentially; each depends on
#         the previous succeeding (a failure halts the rest).
# False = request all of them in parallel (independent of each other).
CHAIN_POST_PROCESSING = True

PRINT_BARCODE_ENABLED = False
PRINTER_ID = ''

'''
To enable barcode printing:
- set PRINT_BARCODE_ENABLED to True
- Set PRINTER_ID to the crucible-label-printer print_id for the printer this
  machine should use (see crucible-label-printer/ansible/inventory.yaml for
  valid IDs, e.g. "b30-113", "ucd1")
- Copy env.sample to .env in this repo and fill in MQTT_PASSWORD (and
  MQTT_BROKER/MQTT_USERNAME/MQTT_PORT if they differ from the defaults)
- Printing is handled by the crucible-label-printer Raspberry Pi fleet over
  MQTT — no local printer driver or setup is needed on this machine
'''
