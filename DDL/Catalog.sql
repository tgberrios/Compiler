-- auto-generated definition
create table catalog
(
    schema_name       varchar not null,
    table_name        varchar not null,
    db_engine         varchar not null,
    connection_string varchar not null,
    active            boolean default true,
    status            varchar default 'full_load'::character varying,
    last_sync_time    timestamp,
    last_sync_column  varchar,
    last_offset       integer default 0,
    cluster_name      varchar,
    constraint catalog_new_pkey
        primary key ()
);

alter table catalog
    owner to "tomy.berrios";

create unique index catalog_new_pkey
    on catalog (schema_name, table_name, db_engine);

