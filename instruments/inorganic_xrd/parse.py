def parse_xrd_file(path: str) -> list[dict]:
    """Parse an inorganic XRD txt file.

    Combined format: line 1 is tab-separated sample names (every other column),
    returning one entry per sample with name from the file and blank uuid.
    Single-sample format: line 1 is a single name, returns one entry.
    """
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        line1 = f.readline().rstrip('\n')

    parts = [p.strip() for p in line1.split('\t')]
    names = [p for p in parts if p]

    if len(names) <= 1:
        name = names[0] if names else ''
        return [{'position': 'S01', 'name': name, 'uuid': '', 'excluded': False}]

    return [
        {'position': f'S{i+1:02d}', 'name': name, 'uuid': '', 'excluded': False}
        for i, name in enumerate(names)
    ]
