import sys
sys.path.insert(0, "/app")
import json
import os
import tempfile
from unittest.mock import patch
os.environ['OPENLINEAGE_ENABLED'] = 'false'
os.environ['QWEN_ENABLED'] = 'true'
os.environ['QWEN_KAFKA_ENABLED'] = 'true'
os.environ['QWEN_INCIDENT_TOPIC'] = 'test.recovery.incidents'
os.environ['QWEN_INCIDENT_DIR'] = tempfile.mkdtemp(prefix='qwen-conditions-')
from common import build_spark
from schema_guard import process_source_batch
from confluent_kafka import Consumer
from qwen_classifier.classifier import QwenClassifier
spark = build_spark('QwenConditionRegression')
root = tempfile.mkdtemp(prefix='qwen-conditions-delta-')
def row(payload):
    return json.dumps({'table_name': 'Test', 'payload': payload})
raws = [row({'a': '1', 'b': '2'}), row({'a': '1'}), row({'a': '1', 'c': '3'}), '{bad json', row({'a': '1', 'b': '2', 'extra': '3'}), row(['not', 'an object'])]
batch = spark.createDataFrame([(r, 'test.conditions') for r in raws], ['raw_value', 'topic'])
valid = []
def clean(frame, batch_id):
    valid.append(frame.count())
# Use the real Ollama call and real broker, with isolated Delta and topic.
process_source_batch(batch, 0, ['a', 'b'], clean, root + '/dead')
assert valid == [1], valid
parked = spark.read.format('delta').load(root + '/dead')
assert parked.count() == 5
assert parked.filter("error_class = 'CORRUPT_RECORD'").count() == 2
# Retry preserves rows; repeated Kafka delivery has the same incident ID.
process_source_batch(batch, 0, ['a', 'b'], lambda frame, bid: None, root + '/dead')
assert spark.read.format('delta').load(root + '/dead').count() == 5
consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': os.path.basename(root), 'auto.offset.reset': 'earliest'})
consumer.subscribe(['test.recovery.incidents'])
messages = []
for _ in range(30):
    msg = consumer.poll(1)
    if msg and not msg.error():
        event = json.loads(msg.value())
        if event['dead_letter_path'] == root + '/dead':
            messages.append(event)
            if len(messages) == 10:
                break
consumer.close()
assert len(messages) == 10, len(messages)
assert len({x['incident_id'] for x in messages}) == 5
assert sum(x['affected_record_count'] for x in {x['incident_id']: x for x in messages}.values()) == 5
assert all(x['classification_status'] == 'OK' for x in messages), messages
os.environ['SILVER1_OPTIONAL_COLUMNS'] = '["b"]'
valid.clear()
with patch('qwen_classifier.integration.capture_exception'):
    process_source_batch(batch, 1, ['a', 'b'], clean, root + '/optional')
assert valid == [2], valid
print('PASS: mixed data, malformed JSON, optional columns, real Qwen, Kafka JSON delivery and retry deduplication')
spark.stop()
