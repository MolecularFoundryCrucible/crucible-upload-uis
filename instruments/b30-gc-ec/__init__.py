NAME = 'b30 - SRI GC #1 N10754'
INGESTOR = ''
INSTRUMENT_ID = 'gc-b30-001'
INSTRUMENT_MFID = '0tmfg17radzgf000w544pxmwer'
UI_MODE = 'preview'
HOLDER_LAYOUTS = {}
DEFAULT_HOLDER_LAYOUT = ''
IS_SESSION = False
FLOW = None
POST_PROCESSING = []
PANEL_TEMPLATE = 'instruments/b30-gc-ec/panel.html'
FILE_PARSER = None

# Scientific metadata field -> request field containing the Crucible sample MFID.
# The preview route resolves these to authoritative sample names/MFIDs and adds
# the corresponding existing samples to the ingestion packet for dataset linking.
SAMPLE_METADATA_FIELDS = {
    'anode_material': 'anode_sample_mfid',
    'cathode_material': 'cathode_sample_mfid',
    'electrolyte': 'electrolyte_sample_mfid',
}
