"""Validate source column names before a fixed Spark schema can hide drift."""
import json
import os


class PayloadSchemaMismatch(ValueError):
    """A source payload violates the expected column-name contract."""


def schema_difference(actual, expected):
    actual, expected = set(actual), set(expected)
    return sorted(expected - actual), sorted(actual - expected)


def schema_error(table, expected_only, source_only):
    return PayloadSchemaMismatch(
        '[SCHEMA_MISMATCH] Payload schema mismatch in table ' + str(table or 'UNKNOWN') + '. '
        + json.dumps({'expected_only': sorted(expected_only), 'source_only': sorted(source_only)})
    )


def process_source_batch(batch, batch_id, expected_columns, write_valid, dead_letter_path,
                         txn_app_id=None, affected_dataset='bronze'):
    """Park schema-invalid raw records, classify each signature, then clean valid rows.

    Storage failures propagate, so the streaming checkpoint cannot skip unsaved data.
    Qwen failures retain a reviewable incident and never release invalid records.
    `txn_app_id` must differ per source when several sources share dead_letter_path
    (see common.idempotent_append); it defaults to SILVER1_TXN_APP_ID.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import MapType, StringType
    from qwen_classifier.integration import capture_exception

    metadata = ['record_id', 'batch_id', 'table_name', 'ingestion_ts', 'payload']
    payload = F.get_json_object('raw_value', '$.payload')
    parsed_object = F.from_json('raw_value', MapType(StringType(), StringType()))
    has_payload = F.array_contains(F.map_keys(parsed_object), 'payload')
    payload_keys = F.map_keys(F.from_json(payload, MapType(StringType(), StringType())))
    flat_keys = F.array_except(
        F.map_keys(F.from_json('raw_value', MapType(StringType(), StringType()))),
        F.array(*[F.lit(x) for x in metadata]),
    )
    keys = F.coalesce(F.when(has_payload, payload_keys).otherwise(flat_keys), F.array())
    expected = F.array(*[F.lit(x) for x in expected_columns])
    # Optional fields still exist in the canonical output, but may be absent at source.
    optional_columns = json.loads(os.getenv('SILVER1_OPTIONAL_COLUMNS', '[]'))
    if not isinstance(optional_columns, list) or any(c not in expected_columns for c in optional_columns):
        raise ValueError('SILVER1_OPTIONAL_COLUMNS must list known canonical columns')
    optional = F.array(*[F.lit(x) for x in optional_columns]).cast('array<string>')
    marked = (batch
              .withColumn('_expected_only', F.array_sort(F.array_except(F.array_except(expected, optional), keys)))
              .withColumn('_source_only', F.array_sort(F.array_except(keys, expected)))
              .withColumn('_detected_error_class', F.when(
                  parsed_object.isNull() | (has_payload & payload_keys.isNull()),
                  F.lit('CORRUPT_RECORD')).otherwise(F.lit('SCHEMA_MISMATCH')))
              .withColumn('_table_name', F.get_json_object('raw_value', '$.table_name'))
              .persist())
    mismatch = ((F.col('_detected_error_class') == 'CORRUPT_RECORD') |
                (F.size('_expected_only') > 0) | (F.size('_source_only') > 0))
    try:
        invalid = marked.filter(mismatch)
        signatures = invalid.groupBy(
            'topic', '_table_name', '_detected_error_class', '_expected_only', '_source_only'
        ).count()
        # Materialize only the small set of schema signatures, not the raw batch.
        issues = list(signatures.toLocalIterator())
        if issues:
            parked = (invalid
                      .withColumn('record_id', F.get_json_object('raw_value', '$.record_id'))
                      .withColumn('source_table', F.col('_table_name'))
                      .withColumn('stage', F.lit('silver1_schema_validation'))
                      .withColumn('error_class', F.col('_detected_error_class'))
                      .withColumn('silver1_batch_id', F.lit(batch_id))
                      .withColumn('healing_status', F.lit('PENDING_LLM'))
                      .withColumn('failed_at', F.current_timestamp()))
            from common import idempotent_append
            app_id = (txn_app_id or os.getenv('SILVER1_TXN_APP_ID', 'silver1')) + '_dead_letter'
            idempotent_append(parked, dead_letter_path, app_id, batch_id)
            for issue in issues:
                exc = (ValueError('[CORRUPT_RECORD] Malformed JSON object in table '
                                  + str(issue._table_name or 'UNKNOWN'))
                       if issue._detected_error_class == 'CORRUPT_RECORD' else
                       schema_error(issue._table_name, issue._expected_only, issue._source_only))
                try:
                    raise exc
                except ValueError as failure:
                    capture_exception(failure, job_name='Silver1SchemaValidation', batch_id=batch_id,
                                      table=issue._table_name, dead_letter_path=dead_letter_path,
                                      context={
                                          'source_topic': issue.topic,
                                          'affected_dataset': affected_dataset,
                                          'detected_error_class': issue._detected_error_class,
                                          'expected_only': issue._expected_only,
                                          'source_only': issue._source_only,
                                          'affected_record_count': issue['count'],
                                      })
            print(f'[silver1] Batch {batch_id}: schema mismatch parked at {dead_letter_path}', flush=True)
        valid = marked.filter(~mismatch).drop('_expected_only', '_source_only', '_table_name', '_detected_error_class')
        if not valid.isEmpty():
            write_valid(valid, batch_id)
    finally:
        marked.unpersist()
