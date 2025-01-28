
import sqlglot
from sqlglot.expressions import (
    CTE,
    Table,
    Subquery,
    Column
)

def parse_query(query: str):
    parsed = sqlglot.parse_one(query)
    info = {}

    # 1. Collect CTE definitions
    #    e.g. WITH cte_alias AS (SELECT ...)
    for cte_expr in parsed.find_all(CTE):
        cte_alias = cte_expr.alias  # Name after 'WITH cte_alias AS (...)'
        info[cte_alias] = {
            "table_name": "CTE",
            "columns": []
        }

    # 2. Collect physical tables (FROM or JOIN references)
    for table_expr in parsed.find_all(Table):
        alias = table_expr.alias or table_expr.name
        # If multiple references to the same alias exist, we just reuse the same dict
        info[alias] = {
            "table_name": table_expr.name,  # actual table name
            "columns": []
        }

    # 3. Collect subquery aliases (SELECT ... ) sub_alias
    for subq_expr in parsed.find_all(Subquery):
        subq_alias = subq_expr.alias
        if subq_alias:
            info[subq_alias] = {
                "table_name": "Subquery",
                "columns": []
            }

    # 4. Collect columns (only those with a table/alias qualifier)
    for col_expr in parsed.find_all(Column):
        if col_expr.table:  # e.g. 'table_name.column1' or 'a.column1'
            alias = col_expr.table
            # If the alias wasn't already seen as a Table/CTE/Subquery, create a new entry
            if alias not in info:
                info[alias] = {
                    "table_name": alias,  # No known physical table name
                    "columns": []
                }
            # Append this qualified column
            info[alias]["columns"].append(col_expr.name)

    # 5. De-duplicate and sort columns for consistent output
    for alias_data in info.values():
        alias_data["columns"] = sorted(set(alias_data["columns"]))
    if len(alias_data['columns'])==0:
        alias_data['columns']=[column.alias_or_name for column in parsed.find_all(Column)]
    return info


# -------------------------------------------------------------------------
# Example driver code (optional) to test each query and show the output
# -------------------------------------------------------------------------
if __name__ == "__main__":
    test_queries = [
        # 1
        "SELECT column1, column2, column3 FROM table_name;",
        # 2
        "SELECT column1, column2 FROM table_name WHERE column3 = 'value';",
        # 3
        "SELECT column1, column2, column3 FROM table_name ORDER BY column1 ASC, column2 DESC;",
        # 4
        "SELECT DISTINCT column1, column2 FROM table_name;",
        # 5
        "SELECT column1, COUNT(column2) FROM table_name GROUP BY column1 HAVING COUNT(column2) > 1;",
        # 6
        "SELECT column1, column2 FROM table_name LIMIT 10;",
        # 7
        "SELECT a.column1, b.column2 FROM table1 a INNER JOIN table2 b ON a.common_column = b.common_column;",
        # 8
        "SELECT a.column1, b.column2 FROM table1 a LEFT JOIN table2 b ON a.common_column = b.common_column;",
        # 9
        "SELECT a.column1, b.column2 FROM table1 a RIGHT JOIN table2 b ON a.common_column = b.common_column;",
        # 10
        "SELECT a.column1, b.column2 FROM table1 a FULL OUTER JOIN table2 b ON a.common_column = b.common_column;",
        # 11
        "SELECT column1, column2 FROM table1 UNION SELECT column1, column2 FROM table2;",
        # 12
        "SELECT column1, column2 FROM table_name WHERE column3 = (SELECT MAX(column3) FROM table_name);",
        # 13
        "SELECT column1, CASE WHEN column2 > 10 THEN 'High' ELSE 'Low' END AS category FROM table_name;",
        # 14
        "SELECT column1, column2 FROM table_name WHERE column3 IN ('value1', 'value2', 'value3');",
        # 15
        "SELECT column1, column2 FROM table_name WHERE column3 BETWEEN '2023-01-01' AND '2023-12-31';",
        # 16
        "SELECT column1, column2 FROM table_name WHERE column3 LIKE 'prefix%';",
        # 17
        "SELECT SUM(column1) AS total, AVG(column2) AS average FROM table_name;",
        # 18
        "SELECT column1, column2, ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY column2) AS row_num FROM table_name;",
        # 19
        "SELECT a.column1, b.column2 FROM table1 a CROSS JOIN table2 b;",
        # 20
        "SELECT column1, column2 FROM table_name WHERE EXISTS (SELECT 1 FROM another_table WHERE another_table.column3 = table_name.column3);",
        # 21
        "SELECT column1, (SELECT COUNT(*) FROM another_table WHERE another_table.column2 = table_name.column1) AS count_column FROM table_name;"
    ]

    for query in test_queries:
        print("*" * 50)
        print(query)
        print("-" * 50)
        info_map = parse_query(query)
        for alias, details in info_map.items():
            print(f"Alias/Table: {alias}")
            print(f"  -> Underlying: {details['table_name']}")
            print(f"  -> Columns: {details['columns']}")
        print()


ouput=["""**************************************************
SELECT column1, column2, column3 FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3']

**************************************************
SELECT column1, column2 FROM table_name WHERE column3 = 'value';
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3']

**************************************************
SELECT column1, column2, column3 FROM table_name ORDER BY column1 ASC, column2 DESC;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3', 'column1', 'column2']

**************************************************
SELECT DISTINCT column1, column2 FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2']

**************************************************
SELECT column1, COUNT(column2) FROM table_name GROUP BY column1 HAVING COUNT(column2) > 1;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column1', 'column2']

**************************************************
SELECT column1, column2 FROM table_name LIMIT 10;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2']

**************************************************
SELECT a.column1, b.column2 FROM table1 a INNER JOIN table2 b ON a.common_column = b.common_column;
--------------------------------------------------
Alias/Table: a
  -> Underlying: table1
  -> Columns: ['column1', 'common_column']
Alias/Table: b
  -> Underlying: table2
  -> Columns: ['column2', 'common_column']

**************************************************
SELECT a.column1, b.column2 FROM table1 a LEFT JOIN table2 b ON a.common_column = b.common_column;
--------------------------------------------------
Alias/Table: a
  -> Underlying: table1
  -> Columns: ['column1', 'common_column']
Alias/Table: b
  -> Underlying: table2
  -> Columns: ['column2', 'common_column']

**************************************************
SELECT a.column1, b.column2 FROM table1 a RIGHT JOIN table2 b ON a.common_column = b.common_column;
--------------------------------------------------
Alias/Table: a
  -> Underlying: table1
  -> Columns: ['column1', 'common_column']
Alias/Table: b
  -> Underlying: table2
  -> Columns: ['column2', 'common_column']

**************************************************
SELECT a.column1, b.column2 FROM table1 a FULL OUTER JOIN table2 b ON a.common_column = b.common_column;
--------------------------------------------------
Alias/Table: a
  -> Underlying: table1
  -> Columns: ['column1', 'common_column']
Alias/Table: b
  -> Underlying: table2
  -> Columns: ['column2', 'common_column']

**************************************************
SELECT column1, column2 FROM table1 UNION SELECT column1, column2 FROM table2;
--------------------------------------------------
Alias/Table: table1
  -> Underlying: table1
  -> Columns: []
Alias/Table: table2
  -> Underlying: table2
  -> Columns: ['column1', 'column2', 'column1', 'column2']

**************************************************
SELECT column1, column2 FROM table_name WHERE column3 = (SELECT MAX(column3) FROM table_name);
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3', 'column3']

**************************************************
SELECT column1, CASE WHEN column2 > 10 THEN 'High' ELSE 'Low' END AS category FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2']

**************************************************
SELECT column1, column2 FROM table_name WHERE column3 IN ('value1', 'value2', 'value3');
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3']

**************************************************
SELECT column1, column2 FROM table_name WHERE column3 BETWEEN '2023-01-01' AND '2023-12-31';
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3']

**************************************************
SELECT column1, column2 FROM table_name WHERE column3 LIKE 'prefix%';
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column3']

**************************************************
SELECT SUM(column1) AS total, AVG(column2) AS average FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2']

**************************************************
SELECT column1, column2, ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY column2) AS row_num FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1', 'column2', 'column1', 'column2']

**************************************************
SELECT a.column1, b.column2 FROM table1 a CROSS JOIN table2 b;
--------------------------------------------------
Alias/Table: a
  -> Underlying: table1
  -> Columns: ['column1']
Alias/Table: b
  -> Underlying: table2
  -> Columns: ['column2']

**************************************************
SELECT column1, column2 FROM table_name WHERE EXISTS (SELECT 1 FROM another_table WHERE another_table.column3 = table_name.column3);
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column3']
Alias/Table: another_table
  -> Underlying: another_table
  -> Columns: ['column3']

**************************************************
SELECT column1, (SELECT COUNT(*) FROM another_table WHERE another_table.column2 = table_name.column1) AS count_column FROM table_name;
--------------------------------------------------
Alias/Table: table_name
  -> Underlying: table_name
  -> Columns: ['column1']
Alias/Table: another_table
  -> Underlying: another_table
  -> Columns: ['column2']
"""]
