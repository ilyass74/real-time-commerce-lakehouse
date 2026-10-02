"""Pure, fail-closed offset handling for the one-source Spark Kafka checkpoint."""
import json
from pathlib import Path


def committed_offsets(checkpoint):
    root = Path(checkpoint)
    commits = root / 'commits'
    batches = [int(p.name) for p in commits.iterdir() if p.is_file() and p.name.isdigit()] if commits.exists() else []
    if not batches:
        raise RuntimeError('No committed batch in the old Bronze checkpoint; stop for investigation')
    batch = max(batches)
    path = root / 'offsets' / str(batch)
    if not path.exists():
        raise RuntimeError('Committed batch offset file is missing: ' + str(batch))
    lines = path.read_text(encoding='utf-8').splitlines()
    if len(lines) != 3 or lines[0] != 'v1':
        raise RuntimeError('Expected a Spark v1 offset log with exactly one source')
    raw = json.loads(lines[2])
    if not isinstance(raw, dict) or not raw:
        raise RuntimeError('Empty or invalid Kafka offsets')
    for topic, parts in raw.items():
        if not isinstance(topic, str) or not isinstance(parts, dict) or not parts:
            raise RuntimeError('Invalid topic offsets')
        for p, value in parts.items():
            if not str(p).isdigit() or type(value) is not int or value < 0:
                raise RuntimeError('Checkpoint must contain explicit nonnegative offsets')
    return batch, raw


def resolve_offsets(committed, bounds, topics):
    expected = set(topics)
    if set(committed) - expected:
        raise RuntimeError('Checkpoint contains unexpected topics')
    if {t for t, p in bounds} != expected:
        raise RuntimeError('Kafka is missing one or more expected topics')
    result = {}
    for topic, parts in committed.items():
        for partition in parts:
            if (topic, int(partition)) not in bounds:
                raise RuntimeError('A checkpoint partition disappeared from Kafka')
    for (topic, partition), (earliest, end) in sorted(bounds.items()):
        value = committed.get(topic, {}).get(str(partition))
        if value is None:
            if earliest != 0:
                raise RuntimeError('An uncaptured partition has already lost history to retention')
            value = 0
        if value < earliest:
            raise RuntimeError(f'Kafka retention gap: {topic}/{partition}: need {value}, earliest {earliest}')
        if value > end:
            raise RuntimeError(f'Kafka offset moved backwards: {topic}/{partition}: need {value}, end {end}')
        result.setdefault(topic, {})[str(partition)] = value
    return result
