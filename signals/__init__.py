import typing as t

from sqlmesh import signal, DatetimeRanges, ExecutionContext


# add the context argument to your function
@signal()
def check_source_readiness(batch: DatetimeRanges,
                           source_table: str,
                           time_column: str,
                           time_column_type: str, # datetime, timestamp, date, etc.
                           context: ExecutionContext) -> t.Union[bool, DatetimeRanges]:

    last_batch = batch[-1] # get the last batch
    end_ts = last_batch[1] # it is still formatted with timezone info, e.g.'2025-05-28 03:55:00+00:00'
    end_ts = end_ts.strftime('%Y-%m-%d %H:%M:%S')
    return len(context.engine_adapter.fetchdf(
        f"SELECT 1 FROM {source_table} WHERE {time_column} > CAST('{end_ts}' AS {time_column_type})")
        ) >= 1
