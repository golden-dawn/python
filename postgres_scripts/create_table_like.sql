create or replace function create_table_like(source_table text, new_table text)
returns void language plpgsql
as $$
declare
    rec record;
begin
    execute format(
        'create table %s (like %s including all)',
        new_table, source_table);
    for rec in
        select oid, conname
        from pg_constraint
        where contype = 'f' 
        and conrelid = source_table::regclass
    loop
        execute format(
            'alter table %s add constraint %s %s',
            new_table,
            replace(rec.conname, source_table, new_table),
            pg_get_constraintdef(rec.oid));
    end loop;
end $$;
