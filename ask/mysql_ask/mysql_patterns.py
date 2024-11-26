from ask.mysql_ask.mysql_helpers import gather_metrics
from ask.mysql_ask.mysql_globals import FIELD_MAPPING, KNOWN_STORE_LOCATIONS, PATTERNS
import re

# Declare global variables for shared state
# FIELD_MAPPING = {}
# KNOWN_STORE_LOCATIONS = {}
# PATTERNS = {}

def generate_column_keywords(table_info):
    """Dynamically generate synonyms for each column."""
    column_keywords = {}

    # Common patterns for synonyms
    keyword_patterns = {
        "id": ["id", "identifier"],
        "category": ["category", "categories", "classification", "product category","department"],
        "product": ["product", "item", "model", "phone model", "products", "phones", "phone"],
        "type": ["type", "kind", "product type", "os", "os type"],
        "location": ["location", "locations" ,"stores" ,"place", "branch", "area", "store_location", "store", "city"],
        "date": ["date", "day", "time", "transaction_date", "launch_date", "release_date"],
        "amount": ["amount", "value", "price", "cost"],
        "price": ["price", "unit price", "cost", "amount","price_usd"],
        "quantity": ["quantity", "transaction_qty", "count", "number"],
        "phone brand": ["phone brand", "brand"],
        "model": ["phone model", "model", "models"],
        "song": ["song", "track", "songs", "tracks"],
        "track": ["song", "track", "songs", "tracks"],
        "name": ["artist", "artists", "singer", "singer names", "artist names", "students", "student names"],
        "department": ["department", "departments", "Department"],
        "gpa": ["gpa", "grade", "score", "gpa score"],
        "gender" :["gender", "Gender"]
    }

    # Iterate through table_info to map columns dynamically
    for column in table_info["numeric"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)

    for column in table_info["categorical"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)

    for column in table_info["date"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)

    return column_keywords


def generate_keywords(column_name, keyword_patterns):
    """Generate synonyms for a single column based on its name."""
    synonyms = [column_name]  # Always include the original column name
    column_name_lower = column_name.lower()

    # Split by underscores or camel case
    tokens = re.split(r"_+", column_name_lower)

    # Match tokens to known patterns
    for token in tokens:
        for key, values in keyword_patterns.items():
            if token in values or key in column_name_lower:
                synonyms.extend(values)

    # Remove duplicates and return
    return list(set(synonyms))


def initialize_patterns(connection, table_name, table_info):
    """Dynamically initialize patterns based on table schema."""
    global FIELD_MAPPING, PATTERNS, KNOWN_STORE_LOCATIONS

    ## TO BE REMOVED
    # Clear and reset globals
    # FIELD_MAPPING.clear()
    # KNOWN_STORE_LOCATIONS.clear()

    # Step 1: Gather table metrics
    # table_info = gather_metrics(connection, table_name)
    ## END TO BE REMOVED

    # Step 2: Generate column keywords and populate FIELD_MAPPING
    column_keywords = generate_column_keywords(table_info)
    # print(f"COLUMN KEYWORDS:\n{column_keywords}")

    for column, synonyms in column_keywords.items():
        for synonym in synonyms:
            FIELD_MAPPING[synonym] = column  # Map each synonym to its corresponding column

    print(f"INVERTED FIELD MAPPING: \n {FIELD_MAPPING}")
    # Debug: Ensure FIELD_MAPPING is populated before further usage
    # print("Debug: Store location after synonym population:", KNOWN_STORE_LOCATIONS)

    # Step 3: Fetch dynamic fields (AFTER FIELD_MAPPING is populated)
    quantity_field = FIELD_MAPPING.get("quantity", "1")  # Fallback to default
    price_field = FIELD_MAPPING.get("price", "unit_price")  # Fallback to default
    product_field = FIELD_MAPPING.get("product", "product")
    product_type_field = FIELD_MAPPING.get("type", "product_type")
    song_field = FIELD_MAPPING.get("song", "track")  # Correct fallback to "track"
    stream_field = FIELD_MAPPING.get("streams", "streams")
    name_field = FIELD_MAPPING.get("name", "artist")
    score_field = FIELD_MAPPING.get("score", "gpa")
    gender_field = FIELD_MAPPING.get("gender", "gender")
    department_field = FIELD_MAPPING.get("department", "Department")
    date_field = FIELD_MAPPING.get("date", "date")
    category_field = FIELD_MAPPING.get("category", "category")
    # Debug the fetched fields
    # print(
    #     f"Quantity Field: {quantity_field}, Price Field: {price_field}, Name Field: {name_field}"
    #     f"Product Field: {product_field}, Song Field: {song_field}, Stream Field: {stream_field}"
    #     f"department_field: {department_field}, gender_field:{gender_field}, score_field:{score_field}"
    # )

    # Check for missing fields and warn
    if "quantity" not in FIELD_MAPPING or "price" not in FIELD_MAPPING:
        print("Warning: Missing 'quantity' or 'price' field in FIELD_MAPPING. Using defaults.")

    # Define static patterns (no placeholders here)
    static_patterns = {
        "total_sales_by_field": {
            "pattern": r".*total (sales|revenue) (by|for each|for every) (\w+)",
            "sql": "SELECT {field}, SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} GROUP BY {field};",
            "description": "This query retrieves the total sales amount for each {field}."
        },
        "total_sales_by_location": {
            "pattern": r"(.*total (sales|revenue) (by|for each|for every|of each) (store location|location|store))|(how much (revenue|sales) does each (store location|location|store) earn\?)",
            "sql": "SELECT {field}, SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} GROUP BY {field};",
            "description": "This query retrieves the total sales amount for each store location."
        },

        # "total_sales_by_date": {
        #     "pattern": r".*total sales (in|on|for|during) ([\w\s,]+)",
        #     "sql": {
        #         "specific_date": "SELECT SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE transaction_date = '{specific_date}';",
        #         "month": "SELECT SUM{quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE DATE_FORMAT(transaction_date, '%M') = '{month}';",
        #         "month_year": "SELECT SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE DATE_FORMAT(transaction_date, '%M %Y') = '{month_year}';",
        #         "year": "SELECT SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE YEAR(transaction_date) = {year};"
        #     },
        #     "description": "This query retrieves the total sales for a specific date, month, year, or timeframe."
        # },

        "total_sales_by_date": {
            "pattern": r".*(total sales|songs released|tracks released) (in|on|for|during) ([\w\s,]+)",
            "sql": {
                "specific_date_sales": "SELECT '{specific_date}',SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE transaction_date = '{specific_date}';",
                "specific_date_tracks": "SELECT '{specific_date}', COUNT(*) AS total_tracks FROM {table_name} WHERE release_date = '{specific_date}';",
                "month_sales": "SELECT '{month}', SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE DATE_FORMAT(transaction_date, '%M') = '{month}';",
                "month_tracks": "SELECT '{month}', COUNT(*) AS total_tracks FROM {table_name} WHERE DATE_FORMAT(release_date, '%M') = '{month}';",
                "year_sales": "SELECT '{year}', SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE YEAR(transaction_date) = {year};",
                "year_tracks": "SELECT '{year}', COUNT(*) AS total_tracks FROM {table_name} WHERE YEAR(release_date) = {year};"
            },
            "description": "This query retrieves the total sales or the total songs released for a specific date, month, year, or timeframe."
        },

        

        "top_best_selling_products": {
            "pattern": r".*top (\d+)\s+(?:best[-\s]?selling)\s+(products|models|items|phones)",
            "sql": "SELECT {product_field}, SUM({quantity_field}) AS total_quantity FROM {table_name} GROUP BY {product_field} ORDER BY total_quantity DESC LIMIT {limit};",
            "description": "This query retrieves the top {limit} best-selling products or models by quantity."
        },
        "top_least_selling_products": {
            "pattern": r".*top (\d+)\s+(?:worst[-\s]?selling)\s+(products|models|items|phones)",
            "sql": "SELECT {product_field}, SUM({quantity_field}) AS total_quantity FROM {table_name} GROUP BY {product_field} ORDER BY total_quantity ASC LIMIT {limit};",
            "description": "This query retrieves the top {limit} best-selling products or models by quantity."
        },

        "top_most_streamed_songs": {
            "pattern": r"(top (\d+)\s+(?:(most|highest)[-\s]?streamed)\s+(tracks|songs|artists))|((song|track|artists)\s+with\s+highest\s+streams)",
            "sql": "SELECT {song_field}, SUM({stream_field}) AS total_quantity FROM {table_name} GROUP BY {song_field} ORDER BY total_quantity DESC LIMIT {limit};",
            "description": "This query retrieves the top {limit} most-streamed tracks or songs."
        },

        "top_least_streamed_songs": {
            "pattern": r"(top (\d+)\s+(?:(least|lowest)[-\s]?streamed)\s+(tracks|songs|artists))|((song|track|artists)\s+with\s+highest\s+streams)",
            "sql": "SELECT {song_field}, SUM({stream_field}) AS total_quantity FROM {table_name} GROUP BY {song_field} ORDER BY total_quantity ASC LIMIT {limit};",
            "description": "This query retrieves the top {limit} most-streamed tracks or songs."
        },

        "specific_product_sales": {
            "pattern": r".*sales of (.+)",
            "sql": "SELECT SUM({quantity_field}) AS total_quantity, SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE {product_field} LIKE '%{product}%' or {category_field} LIKE '%{product}%';",
            "description": "This query retrieves the total quantity and sales of a specific product."
        },
        "total_revenue_by_store": {
            "pattern": r".*total (revenue|sales) (for the store in|in) (\w+)",
            "sql": "SELECT '{store}',SUM({quantity_field} * {price_field}) AS total_sales FROM {table_name} WHERE store_location = '{store}';",
            "description": "This query retrieves the total revenue for the store in {store}."
        },
        "quantity_by_category_in_location": {
            "pattern": r".*quantity of products sold by category in (\w+(?:'?\w+)?)",
            "sql": "SELECT product_category, SUM({quantity_field}) AS total_quantity FROM {table_name} WHERE store_location = '{location}' GROUP BY product_category;",
            "description": "This query retrieves the quantity of products sold by category in {location}."
        },
        "quantity_by_location": {
            "pattern": r".*quantity of products sold in (\w+(?:'?\w+)?)",
            "sql": "SELECT store_location, SUM({quantity_field}) AS total_quantity FROM {table_name} WHERE store_location = '{location}' GROUP BY store_location;",
            "description": "This query retrieves the total quantity of products sold in {location}."
        },

        "average_price": {
            "pattern": r".*average price (of|for|for each|by) (\w+)",
            "sql": "SELECT {field}, AVG({price_field}) AS avg_price FROM {table_name} GROUP BY {field};",
            "description": "This query calculates the average price for each {field}."
        },

        "most_expensive": {
            "pattern": r".*most expensive (phone|product|item|model)",
            "sql": "SELECT {product_field}, {price_field} FROM {table_name} WHERE {price_field} = (SELECT MAX({price_field}) FROM {table_name});",
            "description": "This query retrieves the most expensive product."
        },

        "least_expensive": {
            "pattern": r"(.*least expensive (product|item))",
            "sql": "SELECT {product_field}, {price_field} FROM {table_name} WHERE {price_field} = (SELECT MIN({price_field}) FROM {table_name});",
            "description": "This query retrieves the least expensive phone."
        },

        "most_streamed_artist": {
            "pattern": r".*(most streamed|highest streamed) artist",
            "sql": "SELECT {name_field}, SUM({stream_field}) AS total_streams FROM {table_name} GROUP BY {name_field} ORDER BY total_streams DESC LIMIT 1;",
            "description": "This query retrieves the artist with the highest total streams."
        },

        "average_streams_by_artist": {
            "pattern": r".*(average streams by artist|average streams by artists|streams average for each artist|average streams for artists)",
            "sql": "SELECT {name_field}, AVG({streams_field}) AS avg_streams FROM {table_name} GROUP BY {name_field};",
            "description": "This query calculates the average number of streams for each artist."
        },

        # "student_with_highest_gpa": {
        #     "pattern": r".*\b(student|students) with (the )?highest gpa\b",
        #     "sql": "SELECT {name_field}, {score_field} FROM {table_name} WHERE {score_field} = (SELECT MAX({score_field}) FROM {table_name});",
        #     "description": "This query retrieves the student with the highest GPA."
        # },

        "top_students_with_highest_gpa": {
            "pattern": r".*\btop(?: (\d+))?\s+((students|student) with highest gpa|(students|student) by gpa|(students|student) with the best gpa)\b",
            "sql": "SELECT {name_field}, {score_field} FROM {table_name} ORDER BY {score_field} DESC LIMIT {limit};",
            "description": "This query retrieves the top {limit} students with the highest GPA."
        },

        "average_gpa_by_category": {
            "pattern": r".*(average gpa by (department|year)|gpa average for each (department|year)|average gpa for (departments|years))",
            "sql": "SELECT {category_field}, AVG({gpa_field}) AS avg_gpa FROM {table_name} GROUP BY {category_field};",
            "description": "This query calculates the average GPA grouped by the specified category (department or year)."
        },

        # "students_count_by_gender": {
        #     "pattern": r".*(number of students by gender|students count grouped by gender|number of (male|female) students|how many (male|female) students)",
        #     "sql": "SELECT {gender_field}, COUNT(*) AS student_count FROM {table_name} {where_clause} GROUP BY {gender_field};",
        #     "description": "This query counts the number of students grouped by gender, or the count of specific genders if mentioned."
        # },

        "students_count_by_gender_and_category": {
           "pattern": r".*(how many|count of)\s+(male|female)\s+students(?: in ([\w\s.]+))?(?: in department ([\w\s.]+))?(?: in year (\d+))?",
            "sql": "SELECT {category_field}, COUNT(*) AS student_count FROM {table_name} " \
                "WHERE {gender_field} = '{gender}' {location_condition} {department_condition} {year_condition} GROUP BY {category_field};",
            "description": "This query counts the number of {gender} students grouped by {category}, filtered by location, department, and/or year if specified."
        }
    }

   # Step 5: Apply replacements for all SQL patterns
    updated_patterns = {}
    for pattern_key, pattern_details in static_patterns.items():
        if isinstance(pattern_details["sql"], dict):  # Handle sub-queries
            updated_sql = {
                sub_key: sub_query.replace("{table_name}", table_name)
                .replace("{quantity_field}", quantity_field)
                .replace("{price_field}", price_field)
                .replace("{product_field}", product_field)
                .replace("{product_type_field}", product_type_field)
                .replace("{song_field}", song_field)
                .replace("{stream_field}", stream_field)
                .replace("{name_field}", name_field)
                .replace("{score_field}", score_field)
                .replace("{gender_field}", gender_field)
                .replace("{department_field}", department_field)
                .replace("{category_field}", category_field)
                for sub_key, sub_query in pattern_details["sql"].items()
            }
            updated_patterns[pattern_key] = {**pattern_details, "sql": updated_sql}
        else:  # Single-query patterns
            updated_patterns[pattern_key] = {
                **pattern_details,
                "sql": pattern_details["sql"]
                .replace("{table_name}", table_name)
                .replace("{quantity_field}", quantity_field)
                .replace("{price_field}", price_field)
                .replace("{product_field}", product_field)
                .replace("{product_type_field}", product_type_field)
                .replace("{song_field}", song_field)
                .replace("{stream_field}", stream_field)
                .replace("{name_field}", name_field)
                .replace("{score_field}", score_field)
                .replace("{gender_field}", gender_field)
                .replace("{department_field}", department_field)
                .replace("{category_field}", category_field)
            }

    # Update PATTERNS globally
    PATTERNS.update(updated_patterns)

    # print("Patterns initialized dynamically based on table schema.")
