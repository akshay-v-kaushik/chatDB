from datetime import datetime
import json
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from ask.mongo_ask.mongo_globals_NLP import FIELD_MAPPING, PATTERNS, KNOWN_STORE_LOCATIONS
from ask.mongo_ask.mongo_helpers_NLP import normalize_date, normalize_location_from_keywords

def parse_query_nltk(user_input, table_info):
    """Parse natural language query into MongoDB query."""
    # Tokenize and process the user input
    tokens = word_tokenize(user_input.lower())
    tagged = nltk.pos_tag(tokens)
    stop_words = set(stopwords.words('english'))
    
    # Remove stop words
    keywords = [word for word, pos in tagged if word not in stop_words]

    # Debugging output for tokenization
    print(f"Tokens: {tokens}")
    print(f"Tagged: {tagged}")
    print(f"Keywords: {keywords}")
      # Handle multi-word keywords first
    normalized_keywords = []
    matched_phrases = set()  # Avoid double-matching tokens in phrases

    for phrase, column in FIELD_MAPPING.items():
        if " " in phrase and phrase in user_input.lower():
            normalized_keywords.append(column)
            matched_phrases.update(phrase.split())  # Mark tokens in the phrase as matched

    # Handle single-word keywords (excluding tokens already matched in phrases)
    for word in keywords:
        if word not in matched_phrases:  # Only process unmatched tokens
            normalized_keywords.append(FIELD_MAPPING.get(word, word))

    # Debugging output for normalized keywords
    print(f"Normalized Keywords: {normalized_keywords}")

    # Iterate over all patterns and try to match
    for pattern_key, pattern_details in PATTERNS.items():

        match = re.search(pattern_details["pattern"], user_input, re.IGNORECASE)
        if match:
            print(f"Matched pattern: {pattern_key}")  # Debugging matched pattern

            if pattern_key == "specific_product_sales":
                product = match.group(1)  # Extract product name
                print(product)
                db_field = None
                for key, value in table_info["categorical"].items():
                    if product.lower() in [v.lower() for v in value['unique_values']]:
                        db_field = key
                        break
                    print(key, value)
                if db_field is None:
                    return None, f"Product '{product}' not found in the database. Please try another product."
                # Replace placeholders in the query
                mongodb_query = []
                match_stage = {"$match": {db_field: product}}

                pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                pipeline_json = pipeline_json.replace("{GROUP_FIELD}", db_field)  # Replace placeholder
                updated_pipeline = json.loads(pipeline_json)
                # Insert the $match stage at the beginning of the pipeline
                mongodb_query = [match_stage] + updated_pipeline
                # print(mongodb_query)
                description = pattern_details["description"].format(product=product)
                return mongodb_query, description
            

            elif pattern_key == "top_best_selling_products" or pattern_key == "top_least_selling_products":
                limit = int(match.group(1))  # Extract the limit (e.g., top 5)
                product = match.group(2)  # Extract the product field
                db_field = FIELD_MAPPING.get(product)
                if db_field is None:
                    return None, f"Product field for '{product}' not found in the database. Please try another product."
                
                # Replace placeholders in the MongoDB query dynamically
                mongodb_query = []

                for stage in pattern_details["mongodb"]:
                    # Directly replace placeholders in the dictionary structure
                    if "$group" in stage:
                        stage["$group"]["_id"] = "$"+db_field  # Replace GROUP_FIELD
                    elif "$limit" in stage:
                        stage["$limit"] = limit  # Replace LIMIT with an integer
                    mongodb_query.append(stage)

                # Format the query description dynamically
                description = pattern_details["description"].format(limit=limit)
                
                return mongodb_query, description

            elif pattern_key == "top_most_streamed_songs" or pattern_key == "top_least_streamed_songs":
                limit = int(match.group(2))  # Extract the limit (e.g., top 5)
                stream = match.group(4)  # Extract the product field
                db_field = FIELD_MAPPING.get(stream)
                if db_field is None:
                    return None, f"Product field for '{stream}' not found in the database. Please try another product."
                
                # Replace placeholders in the MongoDB query dynamically
                mongodb_query = []

                for stage in pattern_details["mongodb"]:
                    # Directly replace placeholders in the dictionary structure
                    if "$group" in stage:
                        stage["$group"]["_id"] = "$"+db_field  # Replace GROUP_FIELD
                    elif "$limit" in stage:
                        stage["$limit"] = limit  # Replace LIMIT with an integer
                    mongodb_query.append(stage)

                # Format the query description dynamically
                description = pattern_details["description"].format(limit=limit)
                
                return mongodb_query, description
            
            

            # TODO: GET THIS WORKING
            elif pattern_key == "total_sales_by_date":
                raw_date = match.group(2).strip()  # Extract the date string from user input
                normalized_date = normalize_date(raw_date)
                
                try:
                    year = int(raw_date)  # Try extracting the year from input (e.g., "2023")
                except ValueError:
                    year = None

                if normalized_date:  # Specific date
                    pipeline_json = json.dumps(pattern_details["mongodb"]["specific_date"])  # Specific date pipeline
                    pipeline_json = pipeline_json.replace("date_field", FIELD_MAPPING["date"])  # Replace date_field
                    pipeline_json = pipeline_json.replace("specific_date", normalized_date)  # Replace specific_date
                    updated_pipeline = json.loads(pipeline_json)
                    mongodb_query = updated_pipeline
                    description = pattern_details["description"].format(date=raw_date)
                    return mongodb_query, description

                else:
                    # Check for month and year patterns
                    month, year = None, year
                    try:
                        if "," in raw_date:  # Example: "January, 2023"
                            month, year = raw_date.split(",")
                            month = datetime.strptime(month.strip(), "%B").month  # Convert month name to number
                            year = int(year.strip())
                        elif raw_date.isdigit():  # Example: "2023" (only year)
                            year = int(raw_date)
                    except Exception:
                        pass  # Parsing failed; fallback to error handling

                    if month and year:  # Month and year provided
                            pipeline_json = json.dumps(pattern_details["mongodb"]["month"])  # Month pipeline
                            pipeline_json = pipeline_json.replace("date_field", FIELD_MAPPING["date"])  # Replace date_field
                            pipeline_json = pipeline_json.replace("month", str(month))  # Replace month
                            pipeline_json = pipeline_json.replace("year", str(year))  # Replace year
                            updated_pipeline = json.loads(pipeline_json)
                            mongodb_query = updated_pipeline
                            description = pattern_details["description"].format(date=raw_date)
                            return mongodb_query, description
                    if year:  # Year provided
                        print(year)
                        pipeline_json = json.dumps(pattern_details["mongodb"]["year"])  # Convert pipeline to JSON string
                        pipeline_json = pipeline_json.replace("date_field", FIELD_MAPPING["date"])  # Replace date_field
                        # pipeline_json = pipeline_json.replace("year", str(year))  # Replace year (no '$' prefix needed)
                        updated_pipeline = json.loads(pipeline_json)  # Parse back to Python dict
                        mongodb_query = updated_pipeline
                        description = pattern_details["description"].format(date=raw_date)
                        return mongodb_query, description


                    else:
                        # Provide feedback if the date could not be parsed
                        return None, f"Date '{raw_date}' not recognized. Try formats like 'January 1, 2023', 'January, 2023', or '2023'."

            # TODO: GET THIS WORKING
            elif pattern_key == "total_sales_by_date_range":
                # Extract start and end date phrases from the matched groups
                start_phrase = match.group(2).strip()
                end_phrase = match.group(3).strip()

                # Normalize the start and end dates
                start_date = normalize_date(start_phrase)
                end_date = normalize_date(end_phrase)

                # Ensure both dates are normalized correctly
                if start_date and end_date:
                    mongodb_query = [
                        {"$match": {
                            "transaction_date": {"$gte": start_date, "$lte": end_date}
                        }},
                        {"$group": {
                            "_id": None,
                            "total_sales": {"$sum": {"$multiply": [
                                f"${FIELD_MAPPING.get('quantity', 'quantity')}",
                                f"${FIELD_MAPPING.get('price', 'price')}"
                            ]}}
                        }}
                    ]
                    description = f"This query retrieves the total sales between {start_date} and {end_date}."
                    return mongodb_query, description
                else:
                    return None, "Could not determine the date range. Please use valid start and end dates."


            elif pattern_key == "total_sales_by_field":
             
                raw_field = match.group(3).lower()  # Ensure lowercase for matching
                db_field = FIELD_MAPPING.get(raw_field)
                if not db_field:
                    print(f"No direct match for '{raw_field}' in FIELD_MAPPING. Trying multi-word handling.")  # Debugging 
                    # Split the raw field into words
                    field_words = raw_field.split()  
                    # Attempt to match the first word of the field
                    db_field = FIELD_MAPPING.get(field_words[0])  
                    if db_field:
                        print()
                    else:
                        # Iterate over all words in the field to find a match
                        for word in field_words:
                            db_field = FIELD_MAPPING.get(word)
                            if db_field:
                                # print(f"Matched using word '{word}' in field '{raw_field}' -> {db_field}")  # Debugging
                                break
                print("db_field",db_field)
                if db_field:
                    mongodb_query = []
                    pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                    pipeline_json = pipeline_json.replace("{GROUP_FIELD}", db_field)  # Replace placeholder
                    updated_pipeline = json.loads(pipeline_json)
                    mongodb_query = updated_pipeline
                    description = pattern_details["description"].format(field=db_field)
                    return mongodb_query, description
                else:
                    # If no match is found, provide feedback with available options
                    return None, f"Field '{raw_field}' not recognized. Try one of: {', '.join(FIELD_MAPPING.keys())}"

            elif pattern_key == "total_sales_by_location":
                location = normalize_location_from_keywords(normalized_keywords, KNOWN_STORE_LOCATIONS)
                if location:
                    mongodb_query = [
                                {
                        "$group": {
                            "_id": f"${FIELD_MAPPING.get('location', 'store')}",  # Dynamically resolve 'location'
                            "total_sales": {
                                "$sum": {
                                    "$multiply": [
                                        f"${FIELD_MAPPING.get('quantity', 1)}" if FIELD_MAPPING.get('quantity', 1) != 1 else 1,
                                        f"${FIELD_MAPPING.get('price', 'price_usd')}"
                                    ]
                                }
                            }
                        }
                    }
                                ]
                    description = pattern_details["description"].format(location=location)
                    return mongodb_query, description
                else:
                    return None, "Could not determine the location. Please specify a valid store."

            elif pattern_key == "average_price_by_field":
                raw_field = match.group(2).lower()  # Ensure lowercase for matching
                db_field = FIELD_MAPPING.get(raw_field)
                if not db_field:
                    print(f"No direct match for '{raw_field}' in FIELD_MAPPING. Trying multi-word handling.")  # Debugging
                    field_words = raw_field.split()  
                    db_field = FIELD_MAPPING.get(field_words[0])  
                    if db_field:
                        print(db_field)
                    else:
                        # Iterate over all words in the field to find a match
                        for word in field_words:
                            db_field = FIELD_MAPPING.get(word)
                            if db_field:
                               break
                print(db_field)
                if db_field:
                    mongodb_query = [
                        {"$group": {
                            "_id": f"${db_field}",
                            "avg_price": {"$avg": f"${FIELD_MAPPING.get('price', 'price')}"}
                        }}
                    ]
                    description = pattern_details["description"].format(field=db_field)
                    return mongodb_query, description
                else:
                    # If no match is found, provide feedback with available options
                    return None, f"Field '{raw_field}' not recognized. Try one of: {', '.join(FIELD_MAPPING.keys())}"
            
            # elif pattern_key == "quantity_by_category":
            #     raw_category = match.group(1).lower()  # Extract the category from the user query (e.g., "Bakery")

            #     # Map the raw category to a database field using FIELD_MAPPING
            #     group_field = FIELD_MAPPING.get(raw_category, None)  # Dynamically resolve the field
            #     if not group_field:
            #         # If no match found, return a helpful error message
            #         return None, f"Category '{raw_category}' not recognized. Available categories: {', '.join(FIELD_MAPPING.keys())}"

            #     # Resolve dynamic fields
            #     quantity_field = FIELD_MAPPING.get("quantity", "transaction_qty")
            #     product_name_field = FIELD_MAPPING.get("product", "product")

            #     # Replace placeholders in the query
            #     pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert pipeline to JSON string
            #     pipeline_json = pipeline_json.replace("${GROUP_FIELD}", group_field)  # Replace GROUP_FIELD placeholder
            #     pipeline_json = pipeline_json.replace("${quantity_field}", quantity_field)  # Replace quantity_field dynamically
            #     pipeline_json = pipeline_json.replace("${name_field}", product_name_field)  # Replace name_field dynamically
            #     updated_pipeline = json.loads(pipeline_json)  # Convert back to Python dict

            #     # Build a description for the query
            #     description = pattern_details["description"].format(location=raw_category)

            #     # Return the constructed query and description
            #     return updated_pipeline, description

            elif pattern_key == "simple_count":
                raw_field = match.group(1).lower()  # Extract the field name provided by the user (e.g., "products")

                # Resolve the field name dynamically
                field_name = FIELD_MAPPING.get(raw_field, raw_field)  # Use FIELD_MAPPING or fallback to the raw field

                # Build the pipeline
                pipeline = [
                    {"$group": {
                        "_id": f"${field_name}",  # Group by the specified field
                        "count": {"$sum": 1}      # Count occurrences of each value
                    }},
                    {"$sort": {"count": -1}}  # Optional: Sort by count descending
                ]

                # Create a description
                description = f"This query retrieves the count of each unique value in the field '{field_name}'."
                return pipeline, description
            
            elif pattern_key == "simple_list":
                raw_field = match.group(1).lower()  # Extract the field name provided by the user (e.g., "products")

                # Resolve the field name dynamically
                field_name = FIELD_MAPPING.get(raw_field, raw_field)  # Use FIELD_MAPPING or fallback to the raw field

                # Build the pipeline
                pipeline =  [
                    # Stage 1: Group by the specified field
                    {
                        "$group": {
                            "_id": f"${field_name}",  # Replace with the field name dynamically
                            "count": {"$sum": 1}      # Count occurrences of each unique value
                        }
                    },
                    # Stage 2: Calculate the total count of unique entries
                    {
                        "$group": {
                            "_id": None,                      # Combine all grouped results
                            "total_count": {"$sum": 1},       # Count total unique values
                            "list": {"$push": {"value": "$_id"}}  # Create a list of grouped entries
                        }
                    },
                    # Stage 3: Project the final result
                    {
                        "$project": {
                            "_id": 0,
                            "total_count": 1,
                            "list": 1
                        }
                    }
                ]

                description = f"This query retrieves the count of each unique value in the field '{field_name}'."
                return pipeline, description
            
            elif pattern_key == "simple_find":
                # Extract groups from the pattern
                field_to_return = match.group(1).lower()  # Field to return (e.g., "product")
                filter_field = match.group(2).lower()  # Field to filter by (e.g., "store_location")
                filter_value = match.group(3).lower()  # Value to filter on (e.g., "Astoria")

                # Resolve dynamic fields from FIELD_MAPPING (if applicable)
                field_to_return_resolved = FIELD_MAPPING.get(field_to_return, field_to_return)
                filter_field_resolved = FIELD_MAPPING.get(filter_field, filter_field)
                for uq in table_info["categorical"][filter_field_resolved]["unique_values"]:
                    if filter_value.lower() in uq.lower():
                        actual_filter_value = uq
                        break
                # Build the MongoDB query dynamically
                pipeline = [
                    # Find query with dynamic filter
                    {"$match": {filter_field_resolved:{ "$eq":actual_filter_value}}},

                    # Project the specific field dynamically
                    {"$project": {field_to_return_resolved: 1, "_id": 0}}
                ]

                # Create a description for the query
                description = (
                    f"This query retrieves the {field_to_return_resolved} where "
                    f"{filter_field_resolved} is '{filter_value}'."
                )

                return pipeline, description


            elif pattern_key == "most_expensive":
                # print("MOSTEXPENSIVE", FIELD_MAPPING)
                # mongodb_query = [
                #     {"$sort": {FIELD_MAPPING.get('price', 'price'): -1}},
                #     {"$limit": 1},
                #     {"$project": {FIELD_MAPPING.get('product', 'product'): 1, "_id": 0}}
                # ]
                mongodb_query = []
                mongodb_query.extend(pattern_details["mongodb"])
                # print(mongodb_query)
                description = pattern_details["description"]
                return mongodb_query, description
            
            
            elif pattern_key == "maximum_value":
                # print("MAX VALUE", FIELD_MAPPING)
                raw_field = match.group(1).lower() 
                field_name = FIELD_MAPPING.get(raw_field, raw_field)
                mongodb_query = []
                pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                pipeline_json = pipeline_json.replace("{FIELD_NAME}", field_name)  # Replace placeholder
                updated_pipeline = json.loads(pipeline_json)
                mongodb_query.extend(updated_pipeline)
                
                description = f"This query retrieves the count of each unique value in the field '{field_name}'."
                return mongodb_query, description

            elif pattern_key == "minimum_value":
                # print("MAX VALUE", FIELD_MAPPING)
                raw_field = match.group(1).lower() 
                field_name = FIELD_MAPPING.get(raw_field, raw_field)
                mongodb_query = []
                pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                pipeline_json = pipeline_json.replace("{FIELD_NAME}", field_name)  # Replace placeholder
                updated_pipeline = json.loads(pipeline_json)
                mongodb_query.extend(updated_pipeline)
                
                description = f"This query retrieves the count of each unique value in the field '{field_name}'."
                return mongodb_query, description
            
            elif pattern_key == "average_value":
                # print("MAX VALUE", FIELD_MAPPING)
                raw_field = match.group(1).lower() 
                field_name = FIELD_MAPPING.get(raw_field, raw_field)
                mongodb_query = []
                pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                pipeline_json = pipeline_json.replace("{FIELD_NAME}", field_name)  # Replace placeholder
                updated_pipeline = json.loads(pipeline_json)
                mongodb_query.extend(updated_pipeline)
                
                description = f"This query retrieves the count of each unique value in the field '{field_name}'."
                return mongodb_query, description

            elif pattern_key == "least_expensive":
                mongodb_query = []
                mongodb_query.extend(pattern_details["mongodb"])
                # print(mongodb_query)
                description = pattern_details["description"]
                return mongodb_query, description

            # elif pattern_key == "top_most_streamed_songs":
            #     if match.group(2):  # Matches patterns like "Top 5 most streamed songs"
            #         limit = int(match.group(2))
            #         mongodb_query = [
            #             {"$group": {
            #                 "_id": f"${FIELD_MAPPING.get('song', 'track')}",
            #                 "total_streams": {"$sum": f"${FIELD_MAPPING.get('streams', 'streams')}"}
            #             }},
            #             {"$sort": {"total_streams": -1}},
            #             {"$limit": limit}
            #         ]
            #         description = pattern_details["description"].format(limit=limit)
            #         return mongodb_query, description
            #     elif match.group(5):  # Matches patterns like "Song with highest streams"
            #         mongodb_query = [
            #             {"$group": {
            #                 "_id": f"${FIELD_MAPPING.get('song', 'track')}",
            #                 "total_streams": {"$sum": f"${FIELD_MAPPING.get('streams', 'streams')}"}
            #             }},
            #             {"$sort": {"total_streams": -1}},
            #             {"$limit": 1}
            #         ]
            #         description = "This query retrieves the song with the highest streams."
                    # return mongodb_query, description


        # 
        #  case when no patterns match
    return None, (
            
        "I couldn't understand your query. Try one of the following examples:\n"
        "- Total sales by category\n"
        "- Total revenue for the store in Manhattan\n"
        "- Top 5 best-selling products"
    )

