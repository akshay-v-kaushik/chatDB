import random
from datetime import datetime, timedelta

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
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    rdate = start_date + timedelta(days=random_days)
    return rdate.strftime("%Y-%m-%d")

query_templates = [

    # 1. Simple Queries
    (lambda collection, column: (
        f"db.{collection}.find({{ {column}: {{ $ne: null }} }}, {{ _id: 0 }}).limit(10)",
        f"Fetches the first 10 documents with the field '{column}' that is not null.",
        {"method": "find", "query": {column: {"$ne": None}}, "projection": {"_id": 0}, "modifiers": {"limit": 10}}
    ), ['any']),
    (lambda collection, column: (
        f"db.{collection}.distinct('{column}')",
        f"Finds all distinct values in the field '{column}'.",
        {"method": "distinct", "query": column}
    ), ['any']),
    (lambda collection, column: (
        f"db.{collection}.find({{}}, {{ _id: 0 }}).sort({{ {column}: 1 }}).limit(10)",
        f"Fetches the first 10 documents sorted by '{column}' in ascending order.",
        {"method": "find", "query": {}, "projection": {"_id": 0}, "modifiers": {"sort": {column: 1}, "limit": 10}}
    ), ['any']),
    
    # 2. Conditional Queries
    (lambda collection, column, min_max: (
        f"db.{collection}.find({{ {column}: {{ $gt: {random_val} }} }}, {{ _id: 0 }})",
        f"Finds documents where the field '{column}' is greater than a random value.",
        {"method": "find", "query": {column: {"$gt": random_val}}, "projection": {"_id": 0}}
    ) if (random_val := random_number(*min_max)) else None, ['numeric']),
    (lambda collection, column, min_max: (
        f"db.{collection}.find({{ {column}: {{ $lt: {random_val} }} }}, {{ _id: 0 }})",
        f"Finds documents where the field '{column}' is less than a random value.",
        {"method": "find", "query": {column: {"$lt": random_val}}, "projection": {"_id": 0}}
    ) if (random_val := random_number(*min_max)) else None, ['numeric']),
    (lambda collection, column, min_max: (
        f"db.{collection}.find({{ {column}: {{ $gte: {random_val1}, $lte: {random_val2} }} }})",
        f"Finds all documents where the field '{column}' is between two random values.",
        {"method": "find", "query": {column: {"$gte": random_val1,
                                              "$lte": random_val2}}}
    ) if ((random_val1 := random_number(min_max[0], (sum(min_max)//2))) and (random_val2 := random_number((sum(min_max)//2), min_max[1]))) else None, ['numeric']),

    # 3. Aggregation Queries
    (lambda collection, column: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: null, total: {{ $sum: '${column}' }} }} }}])",
        f"Calculates the total sum of the field '{column}'.",
        {"method": "aggregate", "pipeline": [{"$group": {"_id": None, "total": {"$sum": f"${column}"}}}]}
    ), ['numeric']),
    (lambda collection, column: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: null, average: {{ $avg: '${column}' }} }} }}])",
        f"Calculates the average value of the field '{column}'.",
        {"method": "aggregate", "pipeline": [{"$group": {"_id": None, "average": {"$avg": f"${column}"}}}]}
    ), ['numeric']),
    (lambda collection, column: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: null, min: {{ $min: '${column}' }}, max: {{ $max: '${column}' }} }} }}])",
        f"Finds the minimum and maximum values of the field '{column}'.",
        {"method": "aggregate", "pipeline": [{"$group": {"_id": None, "min": {"$min": f"${column}"}, "max": {"$max": f"${column}"}}}]}
    ), ['numeric']),
    
   # 4. Group by Queries
   (lambda collection, column: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: '${column}', count: {{ $sum: 1 }} }} }}])",
        f"Groups documents by the field '{column}' and counts the occurrences in each group.",
        {"method": "aggregate", "pipeline": [{"$group": {"_id": f"${column}", "count": {"$sum": 1}}}]}
    ), ['categorical']),
    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: '${columns[0]}', total: {{ $sum: '${columns[1]}' }} }} }}, {{ $sort: {{ total: -1 }} }}])",
        f"Groups documents by '{columns[0]}' and calculates the total sum of '{columns[1]}', sorted in descending order.",
        {"method": "aggregate", "pipeline": [{"$group": {"_id": f"${columns[0]}", "total": {"$sum": f"${columns[1]}"}}},
                                             {"$sort": {"total": -1}}]}
    ), ['categorical', 'numeric']),
    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: '${columns[0]}', maxValue: {{ $max: '${columns[1]}' }} }} }}])",
        f"Groups documents by '{columns[0]}' and calculates the maximum value of '{columns[1]}' in each group.",
        {"method": "aggregate", 
         "pipeline": [{"$group": {"_id": f"${columns[0]}", "maxValue": {"$max": f"${columns[1]}"}}}]}
    ), ['categorical', 'numeric']),

    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: '${columns[0]}', avgValue: {{ $avg: '${columns[1]}' }} }} }}])",
        f"Groups documents by '{columns[0]}' and calculates the average value of '{columns[1]}' in each group.",
        {"method": "aggregate", 
         "pipeline": [{"$group": {"_id": f"${columns[0]}", "avgValue": {"$avg": f"${columns[1]}"}}}]}
    ), ['categorical', 'numeric']),
    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: '${columns[0]}', minValue: {{ $min: '${columns[1]}' }} }} }}])",
        f"Groups documents by '{columns[0]}' and calculates the minimum value of '{columns[1]}' in each group.",
        {"method": "aggregate", 
         "pipeline": [{"$group": {"_id": f"${columns[0]}", "minValue": {"$min": f"${columns[1]}"}}}]}
    ), ['categorical', 'numeric']),
    (lambda collection, columns, min_max: (
        f"db.{collection}.aggregate([{{ $match: {{ {columns[1]}: {{ $gte: {random_val1}, $lte: {random_val2} }} }} }}, {{ $group: {{ _id: '${columns[0]}', avgValue: {{ $avg: '${columns[1]}' }}, count: {{ $sum: 1 }} }} }}])",
        f"Filters documents by '{columns[1]}' being in a specific range, groups by '{columns[0]}', and calculates the average and count for each group.",
        {"method": "aggregate", "pipeline": [{"$match": {columns[1]: {"$gte":random_val1, "$lte": random_val2}}},
                                             {"$group": {"_id": f"${columns[0]}", "avgValue": {"$avg": f"${columns[1]}"}, "count": {"$sum": 1}}}]}
    ) if ((random_val1 := random_number(min_max[0], (sum(min_max)//2))) and (random_val2 := random_number((sum(min_max)//2), min_max[1]))) else None, ['categorical', 'numeric']),
    # 5. Date Queries
    (lambda collection, column, date_range: (
        f"db.{collection}.find({{ {column}: {{ $gte: '{random_val}' }} }})",
        f"Fetches documents where the field '{column}' is greater than a random date.",
        {"method": "find", "query": {column: {"$gte": random_val}}}
    ) if (random_val := random_date(*date_range)) else None, ['date']),
    (lambda collection, column, date_range: (
        f"db.{collection}.find({{ {column}: {{ $lte: '{random_val}' }} }})",
        f"Fetches documents where the field '{column}' is less than a random date.",
        {"method": "find", "query": {column: {"$lte": random_val}}}
    ) if (random_val := random_date(*date_range)) else None, ['date']),
    # Advanced Queries
    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $project: {{ {columns[0]}: 1, {columns[1]}: 1, _id: 0 }} }}, {{ $group: {{ _id: '${columns[0]}' }} }}, {{ $sort: {{ total: -1 }} }}])",
        f"Projects '{columns[0]}' and '{columns[1]}', groups by '{columns[0]}', and prints {columns[1]} in descending order.",
        {"method": "aggregate",
         "pipeline": [
             {"$project": {columns[0]: 1, columns[1]: 1, "_id": 0}},
             {"$group": {"_id": f"${columns[0]}"}},
             {"$sort": {"total": -1}}
         ]}
    ), ['categorical', 'others']),
    (lambda collection, columns, min_max: (
        f"db.{collection}.aggregate([{{ $match: {{ {columns[0]}: {{ $lt: {random_val} }} }} }}, {{ $project: {{ {columns[1]}: 1, _id: 0 }} }}, {{ $sort: {{ {columns[0]}: 1 }} }}])",
        f"Filters documents where '{columns[0]}' is less than a random value, projects only the '{columns[1]}' field, and sorts in ascending order.",
        {"method": "aggregate",
         "pipeline": [
             {"$match": {columns[0]: {"$lt": random_val}}},
             {"$project": {columns[1]: 1, "_id": 0}},
             {"$sort": {columns[0]: 1}}
         ]}
    ) if (random_val := random_number(*min_max)) else None, ['numeric', 'others']),
    (lambda collection, columns: (
        f"db.{collection}.aggregate([{{ $group: {{ _id: {{ field1: '${columns[0]}', field2: '${columns[1]}' }}, count: {{ $sum: 1 }} }} }}, {{ $sort: {{ count: -1 }} }}])",
        f"Groups documents by the combination of '{columns[0]}' and '{columns[1]}', and counts the number of documents in each group, sorted by count in descending order.",
        {"method": "aggregate",
         "pipeline": [
             {"$group": {"_id": {"field1": f"${columns[0]}", "field2": f"${columns[1]}"}, "count": {"$sum": 1}}},
             {"$sort": {"count": -1}}
         ]}
    ), ['categorical', 'categorical']),
]