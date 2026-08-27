from .parse import parse_xrd_file

NAME = 'inorganic_xrd'
INGESTOR = ''
UI_MODE = 'multi_assignment'
DEFAULT_SCHEMA = ''
HOLDER_LAYOUTS = {}
DEFAULT_HOLDER_LAYOUT = ''
FLOW = None
POST_PROCESSING = []
PANEL_TEMPLATE = 'instruments/inorganic_xrd/panel.html'
FILE_PARSER = parse_xrd_file
