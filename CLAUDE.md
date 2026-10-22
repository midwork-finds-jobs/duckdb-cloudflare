# duckdb-cloudflare - DuckDB extension for Cloudflare D1 and R2 SQL

Table functions `d1_*` / `r2_sql_*`, secret types `d1` / `r2_sql`, and `ATTACH ... (TYPE d1)`.

## Do not remove the whole release between builds

```sh
# This is much slower
$ rm -rf build/release && make release GEN=ninja

# Than this:
$ make release GEN=ninja
```

Only delete the `build` folder if you encounter weird hard to debug issues.

## Implementation details

Do not create a new http client. You want to autoload `httpfs` in the extension and then use `httputil`. Something like this:

```c++
// In your Extension::Load function
void MyExtension::Load(DuckDB &db) {
    Connection con(db);
    con.BeginTransaction();

    // Auto-load httpfs
    auto result = con.Query("INSTALL httpfs; LOAD httpfs;");
    if (result->HasError()) {
        throw std::runtime_error("Failed to load httpfs: " + result->GetError());
    }

    con.Commit();
    // ... rest of load logic
}
```
