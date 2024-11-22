import random
from datetime import timedelta

def random_number(min_val, max_val): 
    if isinstance(min_val, int) and isinstance(max_val, int):
        # Return an integer within the range
        return random.randint(min_val, max_val)
    elif isinstance(min_val, float) or isinstance(max_val, float):
        # Return a decimal within the range
        return random.uniform(min_val, max_val)
    else:
        raise ValueError("min_val and max_val must be either int or float")

def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

query_templates = [
    # Simple Selects
    (lambda table, column: (f"SELECT `{column}` FROM {table};", f"Selects the  column '`{column}`' from the table '{table}'."), ['any/*']),
    (lambda table, column: (f"SELECT DISTINCT `{column}` FROM {table};", f"Selects distinct values from the column '`{column}`' in the table '{table}'."), ['any/*']),
    (lambda table, column: (f"SELECT `{column}` FROM {table} ORDER BY `{column}` DESC;", f"Selects the top 5 rows of the column '`{column}`' ordered by descending values."), ['any']),
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` > {random_number(*min_max)};", f"Selects rows from '{table}' where '`{column}`' is greater than a random value, limiting to 5 results."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` < {random_number(*min_max)};", f"Selects rows from '{table}' where '`{column}`' is less than a random value, limiting to 5 results."), ['numeric']),

    # Multi-Column Queries
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table};", f"Selects the first 5 rows of columns '`{columns[0]}`' and '`{columns[1]}`' from '{table}'."), ['any', 'any']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} ORDER BY `{columns[1]}` DESC;", f"Selects '`{columns[0]}`' and '`{columns[1]}`' ordered by '`{columns[1]}`' in descending order, limiting to 5 rows."), ['categorical/others/any', 'numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` > {random_number(*min_max)};", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' is greater than a random value, limited to 10 results."), ['others/any', 'numeric/date']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} ORDER BY `{columns[0]}` DESC, `{columns[1]}` ASC;", f"Selects '`{columns[0]}`' and '`{columns[1]}`' ordered by '`{columns[0]}`' descending and '`{columns[1]}`' ascending, limited to 5 rows."), ['any/others', 'numeric/date']),

    # Aggregate Functions (SUM, AVG, MIN, MAX)
    (lambda table, column: (f"SELECT SUM(`{column}`) FROM {table};", f"Calculates the sum of all values in the column '`{column}`' from the table '{table}'."), ['numeric']),
    (lambda table, column: (f"SELECT AVG(`{column}`) FROM {table};", f"Calculates the average value of the column '`{column}`' in the table '{table}'."), ['numeric']),
    (lambda table, column: (f"SELECT MIN(`{column}`), MAX(`{column}`) FROM {table};", f"Finds the minimum and maximum values of the column '`{column}`' in '{table}'."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT SUM(`{column}`) FROM {table} WHERE `{column}` > {random_number(*min_max)};", f"Calculates the sum of values in '`{column}`' where '`{column}`' is greater than a random value."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT AVG(`{column}`) FROM {table} WHERE `{column}` < {random_number(*min_max)};", f"Calculates the average of values in '`{column}`' where '`{column}`' is less than a random value."), ['numeric']),

    # Multiple Aggregates with HAVING and Group By
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, COUNT(*), SUM(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}`,`{columns[1]}` HAVING `{columns[1]}` > {random_number(*min_max)};", f"Groups by '`{columns[0]}`', counts rows, and sums '`{columns[1]}`' where '`{columns[1]}`' is greater than a random value."), ['categorical/others', 'numeric']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, MIN(`{columns[1]}`), MAX(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}`;", f"Groups by '`{columns[0]}`' and finds the minimum and maximum values of '`{columns[1]}`'."), ['categorical/others', 'numeric']),
    
    # Counting Rows and Distinct Counts
    (lambda table, column: (f"SELECT COUNT(*) FROM {table};", f"Counts the total number of rows in the table '{table}'."), ['any']),
    (lambda table, column: (f"SELECT `{column}`, COUNT(*) FROM {table} GROUP BY `{column}`;", f"Counts occurrences of each unique value in '`{column}`' by grouping the rows."), ['categorical']),
    (lambda table, column: (f"SELECT COUNT(DISTINCT `{column}`) FROM {table};", f"Counts the number of distinct values in the column '`{column}`' in '{table}'."), ['any']),
    (lambda table, column, min_max: (f"SELECT `{column}`, COUNT(*) FROM {table} WHERE `{column}` > {random_number(*min_max)} GROUP BY `{column}`;", f"Groups by '`{column}`' where '`{column}`' is greater than a random value and counts each group."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT COUNT(DISTINCT `{column}`) FROM {table} WHERE `{column}` < {random_number(*min_max)};", f"Counts distinct values in '`{column}`' where '`{column}`' is less than a random value."), ['numeric']),

    # Group By with Aggregates
    (lambda table, columns: (f"SELECT `{columns[0]}`, AVG(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}` ORDER BY AVG(`{columns[1]}`) DESC;", f"Groups by '`{columns[0]}`', calculates the average of '`{columns[1]}`', and orders by average in descending order."), ['categorical/others','numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, SUM(`{columns[1]}`) FROM {table} WHERE `{columns[1]}` > {random_number(*min_max)} GROUP BY `{columns[0]}`;", f"Groups by '`{columns[0]}`' where '`{columns[1]}`' is greater than a random value, and calculates the sum."), ['categorical/others','numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, AVG(`{columns[1]}`) FROM {table} WHERE `{columns[1]}` < {random_number(*min_max)} GROUP BY `{columns[0]}`;", f"Groups by '`{columns[0]}`' where '`{columns[1]}`' is less than a random value, and calculates the average for each group."), ['categorical/others','numeric']),

    # Conditional Queries (Using Random Values for Conditions)
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` > {random_number(*min_max)} ORDER BY `{column}` DESC;", f"Selects rows from '{table}' where '`{column}`' is greater than a random value, ordered by '`{column}`' in descending order."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT * FROM {table} WHERE `{column}` > {random_number(*min_max)} ORDER BY `{column}` DESC;", f"Selects rows from '{table}' where '`{column}`' is greater than a random value, ordered by '`{column}`' in descending order."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` < {random_number(*min_max)} ORDER BY `{column}` ASC;", f"Selects rows from '{table}' where '`{column}`' is less than a random value, ordered by '`{column}`' in ascending order."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT * FROM {table} WHERE `{column}` < {random_number(*min_max)} ORDER BY `{column}` ASC;", f"Selects rows from '{table}' where '`{column}`' is less than a random value, ordered by '`{column}`' in ascending order."), ['numeric']),
    (lambda table, column: (f"SELECT `{column}` FROM {table} WHERE `{column}` IS NOT NULL;", f"Selects rows from '{table}' where the column '`{column}`' is not null."), ['any']),
    (lambda table, column: (f"SELECT * FROM {table} WHERE `{column}` IS NOT NULL;", f"Selects rows from '{table}' where the column '`{column}`' is not null."), ['any']),
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` BETWEEN {random_number(min_max[0], (sum(min_max)//2))} AND {random_number((sum(min_max)//2), min_max[1])};", f"Selects rows where '`{column}`' falls between two random values."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT * FROM {table} WHERE `{column}` BETWEEN {random_number(min_max[0], (sum(min_max)//2))} AND {random_number((sum(min_max)//2), min_max[1])};", f"Selects rows where '`{column}`' falls between two random values."), ['numeric']),

    # Date Queries (Using Random Date for Filtering)
    (lambda table, column, date_range: (f"SELECT * FROM {table} WHERE `{column}` >= '{random_date(*date_range).strftime('%Y-%m-%d')}' ORDER BY `{column}`;", f"Selects rows from '{table}' with dates on or after a random date in the column '`{column}`'."), ['date']),
    (lambda table, column, date_range: (f"SELECT * FROM {table} WHERE `{column}` <= '{random_date(*date_range).strftime('%Y-%m-%d')}' ORDER BY `{column}` DESC;", f"Selects rows from '{table}' with dates on or before a random date in the column '`{column}`', ordered by date descending."), ['date']),
    (lambda table, column, date_range: (f"SELECT YEAR(`{column}`), COUNT(*) FROM {table} WHERE `{column}` BETWEEN '{random_date(*date_range).strftime('%Y-%m-%d')}' AND '{random_date(*date_range).strftime('%Y-%m-%d')}' GROUP BY YEAR(`{column}`);", f"Groups by year extracted from '`{column}`', filtering by a random date range and counts rows for each year."), ['date']),
    (lambda table, columns, date_range: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` >= '{random_date(*date_range).strftime('%Y-%m-%d')}' AND `{columns[1]}` <= '{random_date(*date_range).strftime('%Y-%m-%d')}';", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' is within a random date range."), ['any', 'date']),

    # Complex Aggregations with HAVING
    (lambda table, column: (f"SELECT `{column}`, COUNT(*) FROM {table} GROUP BY `{column}` HAVING COUNT(*) > 10;", f"Groups by '`{column}`' and only returns groups with more than 10 occurrences."), ['categorical']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, SUM(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}` HAVING SUM(`{columns[1]}`) > {random_number(*min_max)};", f"Groups by '`{columns[0]}`' and only returns groups where the sum of '`{columns[1]}`' exceeds a random value."), ['categorical/any','numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, AVG(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}` HAVING AVG(`{columns[1]}`) > {random_number(*min_max)};", f"Groups by '`{columns[0]}`' and only returns groups where the average of '`{columns[1]}`' exceeds a random value."), ['categorical/any', 'numeric']),
    
    # # Subqueries
    (lambda table, column: (f"SELECT `{column}` FROM {table} WHERE `{column}` = (SELECT MAX(`{column}`) FROM {table});", f"Selects rows from '{table}' where '`{column}`' equals the maximum value of the column."), ['numeric']),
    (lambda table, column: (f"SELECT `{column}` FROM {table} WHERE `{column}` = (SELECT MIN(`{column}`) FROM {table});", f"Selects rows from '{table}' where '`{column}`' equals the minimum value of the column."), ['numeric']),
    (lambda table, column, min_max: (f"SELECT `{column}` FROM {table} WHERE `{column}` > (SELECT AVG(`{column}`) FROM {table} WHERE `{column}` BETWEEN {random_number(min_max[0], (sum(min_max)//2))} AND {random_number((sum(min_max)//2), min_max[1])});", f"Selects rows from '{table}' where '`{column}`' is equal to the average of the column within a random range."), ['numeric']),

    # Aggregates with Multiple Columns
    (lambda table, columns: (f"SELECT `{columns[0]}`, AVG(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}`;", f"Groups by '`{columns[0]}`' and calculates the average of '`{columns[1]}`'."), ['categorical/others', 'numeric']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, SUM(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}` ORDER BY SUM(`{columns[1]}`) DESC;", f"Groups by '`{columns[0]}`', calculates the sum of '`{columns[1]}`', and orders by sum in descending order."), ['categorical', 'numeric']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, AVG(`{columns[1]}`), COUNT(*) FROM {table} GROUP BY `{columns[0]}` ORDER BY AVG(`{columns[1]}`) DESC;", f"Groups by '`{columns[0]}`', calculates the average of '`{columns[1]}`', and orders by average descending."), ['categorical', 'numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, SUM(`{columns[1]}`) FROM {table} GROUP BY `{columns[0]}`,`{columns[1]}` HAVING SUM(`{columns[1]}`) > {random_number(*min_max)};", f"Groups by '`{columns[0]}`', calculates the sum of '`{columns[1]}`', and returns groups where the sum exceeds a random value."), ['categorical', 'numeric']),

    # # Conditional Queries with Multiple Columns
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` > {random_number(*min_max)};", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' is greater than a random value."), ['any/*', 'numeric']),
    (lambda table, columns, date_range: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` BETWEEN '{random_date(*date_range).strftime('%Y-%m-%d')}' AND '{random_date(*date_range).strftime('%Y-%m-%d')}' ORDER BY `{columns[0]}`;", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' falls within a random date range, ordered by '`{columns[0]}`'."), ['any', 'date']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` BETWEEN {random_number(min_max[0], (sum(min_max)//2))} AND {random_number((sum(min_max)//2), min_max[1])};", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' falls between two random values."), ['any', 'numeric']),
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[0]}` IS NOT NULL AND `{columns[1]}` IS NOT NULL;", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where both columns are not NULL, limited to 10 rows."), ['any', 'any']),

    # # Multi-Column with Subquery
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` = (SELECT MAX(`{columns[1]}`) FROM {table});", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' equals the maximum value."), ['any', 'numeric']),

    # # Multi-column query with MAX value subquery
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` = (SELECT MAX(`{columns[1]}`) FROM {table} WHERE `{columns[0]}` IS NOT NULL);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' equals the maximum value among non-null '`{columns[0]}`' values."), ['any', 'numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` = (SELECT MAX(`{columns[1]}`) FROM {table} WHERE `{columns[1]}` > {random_number(*min_max)});", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' equals the maximum value among non-null '`{columns[0]}`' values."), ['any', 'numeric']),

    # # Multi-column query with MIN value subquery and additional condition
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` = (SELECT MIN(`{columns[1]}`) FROM {table}) AND `{columns[0]}` IS NOT NULL;", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' equals the minimum value, and '`{columns[0]}`' is not NULL."), ['any', 'numeric']),
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` = (SELECT MIN(`{columns[1]}`) FROM {table}) AND `{columns[1]}` <= {random_number(*min_max)};", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' equals the minimum value, and '`{columns[0]}`' is not NULL."), ['any', 'numeric']),

    # # Multi-column query using EXISTS with a subquery condition
    (lambda table, columns, min_max: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE EXISTS (SELECT 1 FROM {table} WHERE `{columns[1]}` > {random_number(*min_max)});", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where there exists a row with '`{columns[1]}`' greater than a random value and matching '`{columns[0]}`' values."), ['any', 'numeric']),

    # # Multi-column subquery with IN clause
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` IN (SELECT `{columns[1]}` FROM {table} WHERE `{columns[0]}` IS NOT NULL);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' values match those in the subquery with non-null '`{columns[0]}`'."), ['any', 'numeric']),

    # # Multi-column subquery with correlated subquery using AVG
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} t1 WHERE `{columns[1]}` > (SELECT AVG(`{columns[1]}`) FROM {table} t2 WHERE t2.`{columns[0]}` = t1.`{columns[0]}`);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' is greater than the average '`{columns[1]}`' for each '`{columns[0]}`' group."), ['categorical', 'numeric']),

    # # Multi-column query with correlated subquery to find rows with a higher value in another column
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} t1 WHERE `{columns[1]}` > (SELECT MAX(`{columns[1]}`) FROM {table} t2 WHERE t2.`{columns[0]}` = t1.`{columns[0]}`);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' is greater than the maximum '`{columns[1]}`' within each '`{columns[0]}`' group."), ['categorical', 'numeric']),

    # # Multi-column subquery with a NOT IN condition
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} WHERE `{columns[1]}` NOT IN (SELECT `{columns[1]}` FROM {table} WHERE `{columns[0]}` IS NULL);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where '`{columns[1]}`' does not match values from rows with NULL '`{columns[0]}`' values."), ['any', 'any']),

    # # Multi-column subquery checking for NULL values in correlated subquery
    (lambda table, columns: (f"SELECT `{columns[0]}`, `{columns[1]}` FROM {table} t1 WHERE EXISTS (SELECT 1 FROM {table} t2 WHERE t2.`{columns[0]}` = t1.`{columns[0]}` AND t2.`{columns[1]}` IS NULL);", f"Selects '`{columns[0]}`' and '`{columns[1]}`' where a NULL value for '`{columns[1]}`' exists in rows with matching '`{columns[0]}`' values."), ['any', 'any']),
]
