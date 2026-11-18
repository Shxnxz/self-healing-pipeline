"""Record a Spark exception as an incident without holding up the batch (Task 109).

capture_exception() first writes the exception, with everything the incident needs, to
a durable outbox file (<QWEN_INCIDENT_DIR>/outbox/<incident_id>.json). Storage errors
propagate, so the batch fails and Spark replays it rather than losing the record. Then
it returns: the model call (up to QWEN_TIMEOUT_SECONDS) and the Kafka delivery (up to
35 s) run on a background worker in the same driver, not inside foreachBatch.

The worker takes the outbox files oldest first: classify, save the incident to
<QWEN_INCIDENT_DIR>/<incident_id>.json, publish it, then delete the outbox file. If
any step fails it keeps the file and tries again later (backoff up to
QWEN_OUTBOX_MAX_BACKOFF_SECONDS), and an incident already classified is not
classified again. A worker claims a file by renaming it first, so two drivers sharing
the folder never take the same one. Files left by a stopped driver are put back and
drained when run_job starts the next one. Delivery stays at least once: consumers deduplicate on incident_id.

QWEN_ASYNC=false keeps the old behaviour: classify, save and publish inside the call,
and a broker failure fails the batch.
"""
import json
import os
import socket
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from .classifier import QwenClassifier
from .handoff import build_incident, incident_id_for, publish_incident, save_incident

OUTBOX = 'outbox'
CLAIMED = '.claimed.'      # <incident_id>.json.claimed.<host>-<pid>: a worker is on it


def _owner():
    return f'{socket.gethostname()}-{os.getpid()}'


def enabled():
    return os.getenv('QWEN_ENABLED', 'false').lower() == 'true'


def asynchronous():
    return os.getenv('QWEN_ASYNC', 'true').lower() != 'false'


def incident_dir():
    return Path(os.getenv('QWEN_INCIDENT_DIR', 'incidents'))


def _record(exc, *, job_name, batch_id, table, dead_letter_path, context):
    trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    record = dict(job_name=job_name, batch_id=batch_id, table=table, error_text=trace,
                  dead_letter_path=dead_letter_path, context=dict(context or {}),
                  created_at=datetime.now(timezone.utc).isoformat())
    record['incident_id'] = incident_id_for(job_name=job_name, batch_id=batch_id, error_text=trace,
                                            dead_letter_path=dead_letter_path, context=record['context'])
    return record


def process(record, directory=None):
    """Classify, save and publish one recorded exception. Returns the incident.
    An incident already saved for the same id is published as it is, not classified again."""
    directory = Path(directory or incident_dir())
    saved = directory / (record['incident_id'] + '.json')
    if saved.exists():
        incident = json.loads(saved.read_text(encoding='utf-8'))
    else:
        classification = QwenClassifier().classify(
            record['error_text'], job_name=record['job_name'], batch_id=record['batch_id'],
            table=record['table'])
        incident = build_incident(classification, error_text=record['error_text'],
                                  dead_letter_path=record['dead_letter_path'], context=record['context'])
        # The id the outbox file is named by (the classifier echoes job and batch, so it
        # is the same id build_incident computes; set it so the two can never drift).
        incident['incident_id'] = record['incident_id']
        incident['created_at'] = record['created_at']
        save_incident(incident, directory)
    publish_incident(incident)
    print(f"[qwen] {record['job_name']} batch={record['batch_id']}: {incident['failure_type']} "
          f"({incident['classification_status']})", file=sys.stderr)
    return incident


class Outbox:
    """The durable queue of exceptions still to classify and publish, and its worker."""

    def __init__(self, directory=None, max_backoff=None):
        self.directory = Path(directory or incident_dir())
        self.path = self.directory / OUTBOX
        self.max_backoff = float(max_backoff if max_backoff is not None
                                 else os.getenv('QWEN_OUTBOX_MAX_BACKOFF_SECONDS', 300))
        self.wake = threading.Event()
        self.thread = None
        self.lock = threading.Lock()

    def put(self, record):
        """Write the record durably (save_incident is atomic). Storage failures propagate."""
        return save_incident(record, self.path)

    def pending(self):
        if not self.path.is_dir():
            return []
        return sorted(self.path.glob('*.json'), key=lambda p: (p.stat().st_mtime, p.name))

    def claim(self, item):
        """Take the file for this process by renaming it (atomic on one file system), so
        two drivers sharing the incidents folder never classify the same incident twice
        (review of #37). Returns the claimed path, or None when another worker took it."""
        claimed = item.with_name(f'{item.name}{CLAIMED}{_owner()}')
        try:
            os.rename(item, claimed)
        except FileNotFoundError:
            return None
        return claimed

    def release(self, claimed):
        """Give a claimed file back to the queue (it failed; a later round retries it)."""
        os.rename(claimed, claimed.with_name(claimed.name.split(CLAIMED)[0]))

    def recover(self, stale_seconds=None):
        """Put back files a stopped worker had claimed: every claim of this host (its
        processes before this start are gone) and any claim older than `stale_seconds`."""
        stale = float(stale_seconds if stale_seconds is not None
                      else os.getenv('QWEN_OUTBOX_STALE_SECONDS', 3600))
        if not self.path.is_dir():
            return 0
        mine, now, recovered = f'{CLAIMED}{socket.gethostname()}-', time.time(), 0
        for claimed in self.path.glob(f'*.json{CLAIMED}*'):
            try:
                if mine in claimed.name or now - claimed.stat().st_mtime > stale:
                    self.release(claimed)
                    recovered += 1
            except FileNotFoundError:
                continue
        return recovered

    def drain(self):
        """Process every pending file once. Returns (done, failed)."""
        done = failed = 0
        for item in self.pending():
            claimed = self.claim(item)
            if claimed is None:
                continue                       # another worker took it
            try:
                record = json.loads(claimed.read_text(encoding='utf-8'))
                process(record, self.directory)
            except Exception as error:          # give the file back, try again later
                failed += 1
                print(f'[qwen] incident {item.stem} not recorded yet: {type(error).__name__}: {error}',
                      file=sys.stderr)
                self.release(claimed)
                continue
            claimed.unlink(missing_ok=True)
            done += 1
        return done, failed

    def _loop(self):
        delay = 1.0
        while True:
            self.wake.clear()
            _, failed = self.drain()
            if failed:
                self.wake.wait(delay)
                delay = min(delay * 2, self.max_backoff)
            else:
                delay = 1.0
                self.wake.wait()

    def start(self):
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.thread = threading.Thread(target=self._loop, name='qwen-outbox', daemon=True)
                self.thread.start()
        self.wake.set()


_outboxes = {}
_outboxes_lock = threading.Lock()


def outbox():
    """The outbox of the current QWEN_INCIDENT_DIR (one worker per directory)."""
    key = str(incident_dir().resolve())
    with _outboxes_lock:
        if key not in _outboxes:
            _outboxes[key] = Outbox(key)
        return _outboxes[key]


def capture_exception(exc, *, job_name, batch_id=None, table=None,
                      dead_letter_path=None, context=None):
    """Record the exception as an incident. Returns the incident (synchronous mode) or
    the outbox record with handoff_status PENDING (default), or None when Qwen is off."""
    if not enabled():
        return None
    record = _record(exc, job_name=job_name, batch_id=batch_id, table=table,
                     dead_letter_path=dead_letter_path, context=context)
    if not asynchronous():
        return process(record)
    box = outbox()
    box.put(record)
    box.start()
    print(f"[qwen] {job_name} batch={batch_id}: incident {record['incident_id'][:12]} queued", file=sys.stderr)
    return {**record, 'handoff_status': 'PENDING'}


def run_job(main, *, job_name):
    """Run a driver. Its crash is recorded synchronously (the process is about to end);
    outbox files a previous driver left behind are drained in the background."""
    if enabled() and asynchronous():
        try:
            box = outbox()
            box.recover()
            if box.pending():
                box.start()
        except Exception as error:
            print(f'[qwen] outbox not started: {type(error).__name__}', file=sys.stderr)
    try:
        return main()
    except Exception as exc:
        try:
            if enabled():
                process(_record(exc, job_name=job_name, batch_id=None, table=None,
                                dead_letter_path=None, context=None))
        except Exception as recording_error:
            # Keep the original job failure; batch callbacks propagate handoff failures.
            print(f'[qwen] Incident recording failed: {type(recording_error).__name__}',
                  file=sys.stderr)
        raise
