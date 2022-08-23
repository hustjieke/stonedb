---
id: limits
sidebar_position: 1.3
---

# Limits
Developed based on MySQL, StoneDB is compatible with the MySQL 5.6 and 5.7 protocols, and the ecosystem, common features, and common syntaxes of MySQL. However, due to characteristics of column-based storage, StoneDB is incompatible with certain MySQL operations and features.
# Unsupported DDL operations
StoneDB does not support the following DDL operations:

- Modify the data type of a field.
- Modify the length of a field.
- Change the character set of a table or a field.
- Convert the character set for a table.
- Optimize a table.
- Analyze a table.
- Lock a table.
- Repair a table.
- Execute a `CREATE TABLE … AS SELECT` statement.
- Reorganize a table.
- Rename a field.
- Configure the default value for a field.
- Specify the default value of a field to null.
- Specify the default value of a field to non-null.
- Add a unique constraint.
- Delete a unique constraint.
- Create an index.
- Remove an index.
- Modify a table comment.

The table attributes and column attributes are difficult to modify. The character sets, data types, constraints, and indexes must be properly defined when tables are being created.
# Unsupported DML operations
StoneDB does not support the following DML operations:

- Execute a `DELETE` statement.
- Use subqueries in an `UPDATE` statement.
- Execute an `UPDATE … JOIN` statement to update multiple tables.
- Execute a `REPLACE … INTO` statement.

StoneDB is not suitable for applications that are frequently updated. It supports only single-table update and insert operations. This is because a column-oriented database needs to find each corresponding column and update the value in the row when processing an update operation. However, a row-oriented database stores data by row. When processing an update operation, the row-oriented database only needs to find the corresponding page or block and update the data directly in the row.
# Unsupported objects
StoneDB does not support the following objects:

- Global indexes
- Unique constraints
- Triggers
- Temporary tables
- Stored procedures containing dynamic SQL statements
- User-defined functions containing nested SQL statements
# Unsupported data types
StoneDB does not support the following data types:

- bit
- enum
- set
- json
- decimal whose precision is higher than 18, for example, decimal(19,x)
- Data types that contain keyword **unsigned** or **zerofill**
# Unsupported binary log formats
StoneDB does not support the following binary log formats:

- row
- mixed

Column-based storage engines support only statement-based binary logs. Row-based binary logs and mixed binary logs are not supported.
# Join queries across storage engines not supported
By default, StoneDB does not support join queries across storage engines. If a join query involves tables in both other storage engines and StoneDB, an error will be reported. You can set the **tianmu_ini_allowmysqlquerypath** parameter to **1 **in the **my.cnf** configuration file to remove this limit.
# Transactions not supported
Transactions must strictly comply with the ACID attributes. However, StoneDB does not support redo and undo logs and thus does not support transactions.
# Partitions not supported
Column-based storage engines do not support partitioning.
# Row locks and table locks not supported
Column-based storage engines do not support row locks or table locks.