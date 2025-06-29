Adding new columns to an existing `taxa`
========================================

In MMOLDB, `taxa` columns contain hard-coded relations representing facts about
baseball. It may also represent facts specific to MMOLB, although as of this
writing there are no examples of that. The key difference between a `taxa` 
table and a `data` or `info` table is that the rows in a `taxa` table are 
fixed.

`taxa` table schema are currently defined at the top of
`migrations/2025-04-26-183010_create_event_table/up.sql`. The rows of the table
are populated by the `mmoldb` Rust app. This allows us to have compile-time
guarantees that all the `taxa` table rows we're using exist.

To add new columns to an existing `taxa`, first find its schema in
`migrations/2025-04-26-183010_create_event_table/up.sql` and add the columns.
Prefer making a column `not null` whenever it does not make sense to have a
missing value for that column. When it does make sense to have a missing value,
add a comment about what a `null` value means. Additional comments about what
the column means are appreciated.

Every column in the database needs to have an associated entry in a 
[Diesel][diesel] `schema` file. The file for the `taxa` schema is 
`src/taxa_schema.rs`. If you're using the devcontainer setup for mmoldb, all 
you need to do is run `diesel database reset` within the app container and the
schema file will be populated based on your changes to the SQL. (Note: this 
command will drop your database.) Otherwise, you currently have to manually add 
the columns to the file. Follow the example of the existing columns. You need 
to use the Diesel type names for the types of the columns, which you can find 
by referencing other columns or by looking [here][diesel-base-types] for 
generic SQL types or [here][diesel-pg-types] for Postgres-specific types. Note 
that columns that are nullable (not declared with `not null`) need to be 
`Nullable<BaseType>` (where `BaseType` is another Diesel type).

Finally, to set the values for your new column(s), find your taxa's definition
in `src/db/taxa.rs`. Each taxa's rows are defined in a Rust enum named 
`TaxaSomething`, where the `Something` is the CamelCase version of your taxa 
table's name. The enum values are the row values, and the values of any 
additional column are specified using a Rust-attribute-like syntax (for Rust
knowers: it's not an actual attribute, it's just a syntax parsed by the `taxa!`
macro). If your taxa's enum doesn't have any `#[]` line above it, add one
following the example of other taxa. Then add your new column within the
square brackets using the format `column_name: rust_type = value`. If you 
don't know the Rust type, you can find it by clicking through on the Diesel
type's documentation page ([standard][diesel-base-types] or 
[postgres-specific][diesel-pg-types]). The corresponding Rust type(s) will be 
listed under "FromSql impls". Note that nullable columns need to be declared 
as `Option<T>`, where `T` is another Rust type.

You must set a value for every row in the Rust enum, otherwise you will get a
(potentially very confusing) compiler error.

Finally, to see your results, follow the "Updating" instructions in the main
readme file.

[diesel]: https://diesel.rs/
[diesel-base-types]: https://docs.diesel.rs/master/diesel/sql_types/index.html
[diesel-pg-types]: https://docs.diesel.rs/master/diesel/pg/sql_types/index.html