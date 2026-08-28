import importlib
import pkgutil
import instruments as _pkg


def _load_all():
    mods = []
    for _, name, _ in pkgutil.iter_modules(_pkg.__path__):
        if name == 'registry':
            continue
        mods.append(importlib.import_module(f'instruments.{name}'))
    return mods


_MODS = _load_all()

INSTRUMENTS               = [m.NAME for m in _MODS]
INSTRUMENT_INGESTORS      = {m.NAME: m.INGESTOR for m in _MODS if m.INGESTOR}
INSTRUMENT_UI_MODE        = {m.NAME: m.UI_MODE for m in _MODS}
INSTRUMENT_HOLDER_LAYOUTS = {m.NAME: m.HOLDER_LAYOUTS for m in _MODS if m.HOLDER_LAYOUTS}
DEFAULT_HOLDER_LAYOUTS    = {m.NAME: m.DEFAULT_HOLDER_LAYOUT for m in _MODS if m.DEFAULT_HOLDER_LAYOUT}
INSTRUMENT_FLOWS          = {m.NAME: m.FLOW for m in _MODS if m.FLOW}
INSTRUMENT_SESSION_MODES  = {m.NAME: m.IS_SESSION for m in _MODS if getattr(m, 'IS_SESSION', None) is not None}
POST_PROCESSING_REQUESTS  = {m.NAME: m.POST_PROCESSING for m in _MODS if m.POST_PROCESSING}
FILE_PARSERS              = {m.NAME: m.FILE_PARSER for m in _MODS if m.FILE_PARSER}
PANEL_TEMPLATES           = {m.NAME: m.PANEL_TEMPLATE for m in _MODS if m.PANEL_TEMPLATE}
