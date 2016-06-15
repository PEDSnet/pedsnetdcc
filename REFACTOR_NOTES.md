Each function or class in its own file ...

### Reasonably independent subcommands/functions:

* populate_observation_period
* check_fact_relationship
* prepdb - look at; if it's ready to go, we can wrap for now, otherwise rewrite

### Classes


Abstract class Transform with methods:

* modify_select (take metadata, table name, and a Select object and return new Select object; site is injected into metadata via `info` data dictionary with key "site")
* new_indexes (take Table object and return list of new Index objects)
* new_constraints (take Table object and return list of new Constraint objects)

ConceptNameTransform

AgeTransform

IdMapTransform

SiteNameTransform

### Functions

add_indexes function - build stock metadata for version, get all indexes from metadata, then for each table invoke the "indexes" method on the list of transformation objections, and append the returned SA Index objects to the list of stock indexes.

undo_constraints

(three functions: one to make the list of indexes, one to generate create statements, one to generate drops. Adding and dropping should ignore "already exists" and "doesn't exist" by default (normal). THree levels of error sensitivity: strict (no errors tolerated; normal - ignore already exists and doesn't exist errors; and force - ignore all errors except connection errors).
)

add_constraints function - similar to previous

undo_constraints

(three functions)

CLI will take a comma-separated list of site names

"execute things in parallel"

"iterate over multiple sites for a given command" (parallel as an option)

"handle NOT NULL constraints"

"move primary key from old table name to new table name"

"Undo transformations" - create table as select stock tables

### Logging
