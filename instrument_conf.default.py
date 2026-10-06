import os
HOME = os.environ.get('HOME')

DEFAULT_BROWSE_DIR = '/Users/mkwall/Documents/Test Data Folders'
DEFAULT_INSTRUMENT_NAME = 'gc-b30-001'

# ID of the crucible-label-printer Raspberry Pi this machine prints barcodes to (see
# that repo's Ansible inventory for valid IDs, e.g. "b30-113", "ucd1"). Leave blank to
# disable barcode printing entirely — the print button/panel won't be shown.
PRINTER_ID = ''

'''
To enable barcode printing:
- Set PRINTER_ID to the crucible-label-printer print_id for the printer this
  machine should use (see crucible-label-printer/ansible/inventory.yaml for
  valid IDs, e.g. "b30-113", "ucd1")
- Copy env.sample to .env in this repo and fill in MQTT_PASSWORD (and
  MQTT_BROKER/MQTT_USERNAME/MQTT_PORT if they differ from the defaults)
- Printing is handled by the crucible-label-printer Raspberry Pi fleet over
  MQTT — no local printer driver or setup is needed on this machine
'''
