create table metadata.transfer_metrics
(
    id                       serial
        primary key,
    schema_name              varchar(100) not null,
    table_name               varchar(100) not null,
    db_engine                varchar(50)  not null,
    records_transferred      bigint,
    bytes_transferred        bigint,
    transfer_duration_ms     integer,
    transfer_rate_per_second numeric(10, 2),
    chunk_size               integer,
    memory_used_mb           numeric(10, 2),
    cpu_usage_percent        numeric(5, 2),
    io_operations_per_second integer,
    transfer_type            varchar(20),
    status                   varchar(20),
    error_message            text,
    started_at               timestamp,
    completed_at             timestamp,
    created_at               timestamp default now(),
    created_date             date generated always as ((created_at)::date) stored,
    constraint unique_table_metrics
        unique (schema_name, table_name, db_engine, created_date)
);

comment on table metadata.transfer_metrics is 'Comprehensive metrics for data transfer operations';

comment on column metadata.transfer_metrics.records_transferred is 'Number of records transferred in this operation';

comment on column metadata.transfer_metrics.bytes_transferred is 'Number of bytes transferred in this operation';

comment on column metadata.transfer_metrics.transfer_duration_ms is 'Duration of transfer operation in milliseconds';

comment on column metadata.transfer_metrics.transfer_rate_per_second is 'Transfer rate in records per second';

comment on column metadata.transfer_metrics.chunk_size is 'Chunk size used for transfer operation';

comment on column metadata.transfer_metrics.memory_used_mb is 'Memory usage during transfer in MB';

comment on column metadata.transfer_metrics.cpu_usage_percent is 'CPU usage percentage during transfer';

comment on column metadata.transfer_metrics.io_operations_per_second is 'I/O operations per second during transfer';

comment on column metadata.transfer_metrics.transfer_type is 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';

comment on column metadata.transfer_metrics.status is 'Transfer status: SUCCESS, FAILED, PARTIAL';

comment on column metadata.transfer_metrics.error_message is 'Error message if transfer failed';

comment on column metadata.transfer_metrics.started_at is 'When the transfer operation started';

comment on column metadata.transfer_metrics.completed_at is 'When the transfer operation completed';

alter table metadata.transfer_metrics
    owner to "tomy.berrios";

create index idx_transfer_metrics_schema_table
    on metadata.transfer_metrics (schema_name, table_name);

create index idx_transfer_metrics_db_engine
    on metadata.transfer_metrics (db_engine);

create index idx_transfer_metrics_status
    on metadata.transfer_metrics (status);

create index idx_transfer_metrics_created_at
    on metadata.transfer_metrics (created_at);

create index idx_transfer_metrics_transfer_type
    on metadata.transfer_metrics (transfer_type);

