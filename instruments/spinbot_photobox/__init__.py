NAME = 'spinbot_photobox'
INGESTOR = ''
INSTRUMENT_ID = 'photobox'
INSTRUMENT_MFID = '0t4ptyzswnyps0006tqctfjbpr'
UI_MODE = 'carrier_photo'
HOLDER_LAYOUTS = {}
DEFAULT_HOLDER_LAYOUT = ''
IS_SESSION = False
# carrier_photo mode doesn't read FLOW — /api/photobox/upload calls its Prefect
# deployment directly (see main.py's photobox_upload_endpoint).
FLOW = None
POST_PROCESSING = ['carrier_segmentation']
CHAIN_POST_PROCESSING = True
PANEL_TEMPLATE = 'instruments/spinbot_photobox/panel.html'
LIVE_PARSER = None
