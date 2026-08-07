create schema if not exists ops_control;

create table if not exists ops_control.pipeline_run (
    pipeline_run_id uuid primary key default gen_random_uuid(),
    pipeline_name text not null,
    run_type text not null,
    status text not null default 'PENDING',
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    failure_category text,
    error_message text,
    created_at timestamp with time zone not null default current_timestamp
);

create table if not exists ops_control.extract_batch (
    extract_batch_id uuid primary key default gen_random_uuid(),

    pipeline_run_id uuid not null
        references ops_control.pipeline_run(pipeline_run_id),

    dataset_id text not null,
    source_url text not null,
    status text not null default 'PENDING',

    page_size integer not null,
    pages_downloaded integer not null default 0,
    record_count bigint not null default 0,

    raw_prefix text,
    manifest_path text,

    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone not null default current_timestamp
);

create index if not exists idx_extract_batch_pipeline_run
    on ops_control.extract_batch(pipeline_run_id);

create table if not exists ops_control.raw_file_manifest (
    raw_file_id uuid primary key default gen_random_uuid(),

    extract_batch_id uuid not null
        references ops_control.extract_batch(extract_batch_id),

    file_name text not null,
    object_path text not null,
    record_count bigint not null,

    file_size_bytes bigint,
    checksum_sha256 text,

    created_at timestamp with time zone not null default current_timestamp,

    unique (extract_batch_id, object_path)
);

create index if not exists idx_raw_file_extract_batch
    on ops_control.raw_file_manifest(extract_batch_id);

create table if not exists ops_control.source_watermark (
    dataset_id text primary key,

    watermark_column text not null,
    watermark_value timestamp with time zone,

    last_successful_pipeline_run_id uuid
        references ops_control.pipeline_run(pipeline_run_id),

    updated_at timestamp with time zone
        not null default current_timestamp
);

create or replace view ops_control.pipeline_run_summary as
select
    pipeline.pipeline_run_id,
    pipeline.pipeline_name,
    pipeline.run_type,
    pipeline.status as pipeline_status,
    pipeline.started_at as pipeline_started_at,
    pipeline.finished_at as pipeline_finished_at,
    case
        when pipeline.started_at is null then null
        else extract(
            epoch from (
                coalesce(
                    pipeline.finished_at,
                    current_timestamp
                ) - pipeline.started_at
            )
        )::bigint
    end as pipeline_duration_seconds,
    pipeline.failure_category,
    pipeline.error_message,
    pipeline.created_at,
    batch.extract_batch_id,
    batch.dataset_id,
    batch.status as batch_status,
    batch.page_size,
    batch.pages_downloaded,
    batch.record_count,
    batch.raw_prefix,
    batch.manifest_path,
    batch.started_at as batch_started_at,
    batch.finished_at as batch_finished_at,
    watermark.watermark_column,
    watermark.watermark_value,
    watermark.last_successful_pipeline_run_id,
    coalesce(
        watermark.last_successful_pipeline_run_id
            = pipeline.pipeline_run_id,
        false
    ) as is_last_successful_run
from ops_control.pipeline_run as pipeline
left join ops_control.extract_batch as batch
    on batch.pipeline_run_id = pipeline.pipeline_run_id
left join ops_control.source_watermark as watermark
    on watermark.dataset_id = batch.dataset_id;
