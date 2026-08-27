import h5py

NAME = 'nirvana'
INGESTOR = 'NirvanaMultiPosLineScanIngestor'
UI_MODE = 'multi_assignment'
DEFAULT_SCHEMA = ''
HOLDER_LAYOUTS = {
    'Tray 2×8': [
        {'label': 'Tray A', 'slots': 8},
        {'label': 'Tray B', 'slots': 8},
    ],
}
DEFAULT_HOLDER_LAYOUT = 'Tray 2×8'
FLOW = None
POST_PROCESSING = []
PANEL_TEMPLATE = 'instruments/nirvana/panel.html'


def _parse_nirvana_h5(path: str) -> list[dict]:
    """Extract sample names and UUIDs from a Nirvana ScopeFoundry h5 file.
    Supports both _line_scan (positions at measurement level) and
    _spec_run (positions nested under dtype groups like pl, uvvis)."""
    def decode(v):
        return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

    samples = []
    with h5py.File(path, 'r') as f:
        meas_grp = f.get('measurement')
        if meas_grp is None:
            raise ValueError("No /measurement group found in this file")
        meas_name = next(iter(meas_grp.keys()))
        meas = meas_grp[meas_name]

        if meas_name.endswith('_spec_run'):
            dtype_key = next(
                (k for k in meas.keys() if k != 'settings' and 'positions' in meas[k]),
                None,
            )
            if dtype_key is None:
                raise ValueError(f"No positions group found in spec_run measurement '{meas_name}'")
            positions = meas[dtype_key]['positions']
        else:
            positions = meas.get('positions')
            if positions is None:
                raise ValueError(f"No positions group found under /measurement/{meas_name}")

        for i, pos_name in enumerate(sorted(positions.keys())):
            attrs = positions[pos_name].attrs
            sample_name = decode(attrs['sample_name']) if 'sample_name' in attrs else pos_name
            sample_uuid = decode(attrs['sample_uuid']) if 'sample_uuid' in attrs else ''
            samples.append({
                'position': f'S{i+1:02d}',
                'name': sample_name,
                'uuid': sample_uuid,
                'excluded': False,
            })
    return samples


FILE_PARSER = _parse_nirvana_h5
