# financial_data_lake

start
↓
log_pipeline_start        # insert pipeline_run, store UUID in XCom
↓
read_csv                  # load CSV, return raw symbol list
↓
sanitize                  # normalize symbols to dot format
↓
query_active_symbols      # psycopg2 query, return active symbol list
↓
diff_symbols              # compare lists, push {new, delisted} dict to XCom
↓
┌──────────────────────────────────────────┐
upsert_membership                    update_exit_date
↓                                         ↓
upsert_universe_membership           log_delisted
└──────────────────────────────────────────┘
↓
log_pipeline_end          # update pipeline_run to SUCCESS, clear XCom
↓
end

on_failure_callback       # updates pipeline_run to FAILED on any task failure
