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
                # print(product)
                db_field = None
                for key, value in table_info["categorical"].items():
                    if product.lower() in [v.lower() for v in value['unique_values']]:
                        db_field = key
                        break
                if db_field is None:
                    return None, f"Product '{product}' not found in the database. Please try another product."
                # Replace placeholders in the query
                mongodb_query = []
                pipeline_json = json.dumps(pattern_details["mongodb"])  # Convert to JSON string for safe replacement
                pipeline_json = pipeline_json.replace("{GROUP_FIELD}", db_field)  # Replace placeholder
                updated_pipeline = json.loads(pipeline_json)
                mongodb_query = updated_pipeline
                # print(mongodb_query)
                description = pattern_details["description"].format(product=product)
                return mongodb_query, description
            

            elif pattern_key == "top_best_selling_products" or pattern_key == "top_least_sellling_products":
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

            elif pattern_key == "top_most_streamed_songs" or pattern_key == "top_least_streamed_tracks":
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
                time_phrase = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", match.group(3))  # Clean ordinal suffixes
                query_type = match.group(1).lower()  # Extract "total sales", "songs released", or "tracks released"

                # Determine whether to use sales or songs SQL
                if query_type in ["total sales"]:
                    sql_key = "specific_date_sales"
                    count_sql_key = "month_sales"
                    year_sql_key = "year_sales"
                else:
                    sql_key = "specific_date_tracks"
                    count_sql_key = "month_tracks"
                    year_sql_key = "year_tracks"

                # Check for specific date format (YYYY-MM-DD)
                if re.match(r"\d{4}-\d{2}-\d{2}", time_phrase):  # Match specific date format
                    query = pattern_details["sql"][sql_key].format(specific_date=time_phrase)
                    description = f"This query retrieves the {query_type} on {time_phrase}."
                    return query, description

                # Specific date query with natural language (e.g., "January 1st")
                normalized_date = normalize_date(time_phrase)
                if normalized_date:
                    mongodb_query = [
                        {"$match": {"transaction_date": normalized_date}},
                        {"$group": {
                            "_id": None,
                            "total_sales": {"$sum": {"$multiply": [
                                f"${FIELD_MAPPING.get('quantity', 'quantity')}",
                                f"${FIELD_MAPPING.get('price', 'price')}"
                            ]}}
                        }}
                    ]
                    description = f"This query retrieves the total sales on {normalized_date}."
                    return mongodb_query, description

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


            elif pattern_key == "most_expensive":
                print("MOSTEXPENSIVE", FIELD_MAPPING)
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

