from .parse import parse_xrd_file

NAME = 'inorganic_xrd'
INGESTOR = 'InorganicXRDIngestor'
INSTRUMENT_ID = 'inorganic-xrd'
INSTRUMENT_MFID = '0tmz7j9xmsrvn000tbs1zjwgpr'
UI_MODE = 'multi_assignment'
HOLDER_LAYOUTS = {}
DEFAULT_HOLDER_LAYOUT = ''
IS_SESSION = False
FLOW = None
POST_PROCESSING = []
CHAIN_POST_PROCESSING = True
PANEL_TEMPLATE = 'instruments/inorganic_xrd/panel.html'
LIVE_PARSER = parse_xrd_file
