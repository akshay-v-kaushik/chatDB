import nltk
# nltk.download('punkt_tab')
# nltk.download('averaged_perceptron_tagger_eng')
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from ask.mysql_ask.mysql_globals import FIELD_MAPPING, PATTERNS, KNOWN_STORE_LOCATIONS
from ask.mysql_ask.mysql_helpers import normalize_date, normalize_location_from_keywords

def parse_query_nltk(user_input):
    """Parse natural language query into SQL query."""
    # Tokenize and process the user input
    tokens = word_tokenize(user_input.lower())
    tagged = nltk.pos_tag(tokens)
    stop_words = set(stopwords.words('english'))
    
    # Remove stop words
    keywords = [word for word, pos in tagged if word not in stop_words]

   
    # Check if FIELD_MAPPING is populated
    if not FIELD_MAPPING:
        return None, "FIELD_MAPPING is empty. Please ensure it is properly populated."

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

    # Iterate over all patterns and try to match
    for pattern_key, pattern_details in PATTERNS.items():

        match = re.search(pattern_details["pattern"], user_input, re.IGNORECASE)
        if match:
           
            # Handle specific patterns with parameters
            if pattern_key == "top_best_selling_products":
                limit = match.group(1)  # Extract the limit (e.g., "5")
                try:
                    limit = int(limit)
                    query = pattern_details["sql"].format(limit=limit)
                    description = pattern_details["description"].format(limit=limit)
                    return query, description
                except ValueError:
                    return None, "Invalid limit specified. Please provide a number (e.g., 'top 5 best-selling products')."

            elif pattern_key == "top_least_selling_products":
                limit = match.group(1)  # Extract the limit (e.g., "5")
                try:
                    limit = int(limit)
                    query = pattern_details["sql"].format(limit=limit)
                    description = pattern_details["description"].format(limit=limit)
                    return query, description
                except ValueError:
                    return None, "Invalid limit specified. Please provide a number (e.g., 'top 5 best-selling products')."

            elif pattern_key == "specific_product_sales":
                product = match.group(1)  # Extract product name
                query = pattern_details["sql"].format(product=product)
                description = pattern_details["description"].format(product=product)
                return query, description

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
                    date_field = FIELD_MAPPING.get("date", "date")
                    query = pattern_details["sql"][sql_key].format(date_field=date_field, specific_date=time_phrase)
                    description = f"This query retrieves the {query_type} on {time_phrase}."
                    return query, description

                # Specific date query with natural language (e.g., "January 1st")
                normalized_date = normalize_date(time_phrase)
                if normalized_date:
                    query = pattern_details["sql"][sql_key].format(date_field=date_field,specific_date=normalized_date)
                    description = f"This query retrieves the {query_type} on {normalized_date}."
                    return query, description

                # Month-year query (e.g., "January 2023")
                words = time_phrase.split()
                if len(words) == 2 and words[0].capitalize() in [
                    "January", "February", "March", "April", "May", "June",
                    "July", "August", "September", "October", "November", "December"
                ]:
                    month_year = f"{words[0].capitalize()} {words[1]}"
                    query = pattern_details["sql"][count_sql_key].format(month=words[0].capitalize())
                    description = f"This query retrieves the {query_type} for {month_year}."
                    return query, description

                # Year-only query (e.g., "2023")
                if len(words) == 1 and words[0].isdigit():
                    query = pattern_details["sql"][year_sql_key].format(year=words[0])
                    description = f"This query retrieves the {query_type} for the year {words[0]}."
                    return query, description

                # Month-only query (e.g., "January")
                if words[0].capitalize() in [
                    "January", "February", "March", "April", "May", "June",
                    "July", "August", "September", "October", "November", "December"
                ]:
                    query = pattern_details["sql"][count_sql_key].format(month=words[0].capitalize())
                    description = f"This query retrieves the {query_type} for {words[0].capitalize()}."
                    return query, description


            elif pattern_key == "total_sales_by_date_range":
                # Extract start and end date phrases from the matched groups
                start_phrase = match.group(2).strip()
                end_phrase = match.group(3).strip()

                # Normalize the start and end dates
                start_date = normalize_date(start_phrase)
                end_date = normalize_date(end_phrase)

                # Ensure both dates are normalized correctly
                if start_date and end_date:
                    query = pattern_details["sql"].format(
                        start_date=start_date,
                        end_date=end_date
                    )
                    description = f"This query retrieves the total sales between {start_date} and {end_date}."
                    return query, description
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
                
                if db_field:
                    # Generate the SQL query using the matched database field
                    query = pattern_details["sql"].format(field=db_field)
                    description = pattern_details["description"].format(field=db_field)
                    return query, description
                else:
                    # If no match is found, provide feedback with available options
                    return None, f"Field '{raw_field}' not recognized. Try one of: {', '.join(FIELD_MAPPING.keys())}"

            elif pattern_key == "total_sales_by_location":
                location = normalize_location_from_keywords(normalized_keywords)
                if location:
                    query = pattern_details["sql"].format(location=location)
                    description = pattern_details["description"].format(location=location)
                    return query, description
                else:
                    return None, "Could not determine the location. Please specify a valid store."

            elif pattern_key == "total_revenue_by_store":
                location = normalize_location_from_keywords(normalized_keywords)
                if not location:
                    return None, "Could not determine the store location. Please specify a valid store."
                query = pattern_details["sql"].format(store=location)
                description = pattern_details["description"].format(store=location)
                return query, description
            
            elif pattern_key == "quantity_by_category_in_location":
                location = normalize_location_from_keywords(normalized_keywords)
                if location:
                    query = pattern_details["sql"].format(location=location)
                    description = pattern_details["description"].format(location=location)
                    return query, description
                else:
                    return None, "Could not determine the location. Please specify a valid store."
            
            elif pattern_key == "quantity_by_location":
                location = normalize_location_from_keywords(normalized_keywords)
                if location:
                    query = pattern_details["sql"].format(location=location)
                    description = pattern_details["description"].format(location=location)
                    return query, description
                else:
                    return None, "Could not determine the location. Please specify a valid store."

            elif pattern_key == "average_price":
             
                raw_field = match.group(2).lower()  # Ensure lowercase for matching
                db_field = FIELD_MAPPING.get(raw_field)
                if not db_field:
                    print(f"No direct match for '{raw_field}' in FIELD_MAPPING. Trying multi-word handling.")  # Debugging
                    field_words = raw_field.split()  
                    db_field = FIELD_MAPPING.get(field_words[0])  
                    if db_field:
                        print()
                    else:
                        # Iterate over all words in the field to find a match
                        for word in field_words:
                            db_field = FIELD_MAPPING.get(word)
                            if db_field:
                                break
                if db_field:
                    # Generate the SQL query using the matched database field
                    query = pattern_details["sql"].format(field=db_field)
                    description = pattern_details["description"].format(field=db_field)
                    return query, description
                else:
                    # If no match is found, provide feedback with available options
                    return None, f"Field '{raw_field}' not recognized. Try one of: {', '.join(FIELD_MAPPING.keys())}"

            elif pattern_key == "most_expensive":
                # Dynamically find the price field
                price_field = FIELD_MAPPING.get("price") or FIELD_MAPPING.get("price_usd")
                if not price_field:
                    return None, "No price field found in the dataset. Ensure a price-related column exists."

                # Generate the SQL query dynamically
                query = pattern_details["sql"].replace("{price_field}", price_field)
                description = pattern_details["description"]
                return query, description
            

            elif pattern_key == "least_expensive":
                # Dynamically find the price field
                price_field = FIELD_MAPPING.get("price") or FIELD_MAPPING.get("price_usd")
                if not price_field:
                    return None, "No price field found in the dataset. Ensure a price-related column exists."

                # Generate the SQL query dynamically
                query = pattern_details["sql"].replace("{price_field}", price_field)
                description = pattern_details["description"]
                return query, description

            elif pattern_key == "top_most_streamed_songs":
                if match.group(2):  # First part of the pattern matches (e.g., "Top 5 most streamed songs")
                    limit = match.group(2)  # Extract the limit
                    try:
                        limit = int(limit)
                        query = pattern_details["sql"].format(
                            song_field=FIELD_MAPPING.get("song", "track"),
                            stream_field=FIELD_MAPPING.get("streams", "streams"),
                            table_name="spotify",
                            limit=limit
                        )
                        description = pattern_details["description"].format(limit=limit)
                        return query, description
                    except ValueError:
                        return None, "Invalid limit specified. Please provide a number (e.g., 'top 5 most streamed songs')."

            elif pattern_key == "top_least_streamed_songs":
                if match.group(2):  # First part of the pattern matches (e.g., "Top 5 most streamed songs")
                    limit = match.group(2)  # Extract the limit
                    try:
                        limit = int(limit)
                        query = pattern_details["sql"].format(
                            song_field=FIELD_MAPPING.get("song", "track"),
                            stream_field=FIELD_MAPPING.get("streams", "streams"),
                            table_name="spotify",
                            limit=limit
                        )
                        description = pattern_details["description"].format(limit=limit)
                        return query, description
                    except ValueError:
                        return None, "Invalid limit specified. Please provide a number (e.g., 'top 5 most streamed songs')."

                elif match.group(5):  # Second part of the pattern matches (e.g., "Song with highest streams")
                    query = pattern_details["sql"].format(
                        song_field=FIELD_MAPPING.get("song", "track"),
                        stream_field=FIELD_MAPPING.get("streams", "streams"),
                        table_name="spotify",
                        limit=1  # Default to a single result
                    )
                    description = "This query retrieves the song with the highest streams."
                    return query, description

            elif pattern_key == "most_streamed_artist":
                query = pattern_details["sql"].format(table_name="spotify")
                description = pattern_details["description"]
                return query, description
            
            elif pattern_key == "top_students_with_highest_gpa":
                match = re.match(pattern_details["pattern"], user_input, re.IGNORECASE)
                if match:
                    limit = match.group(1)  # Extract the limit (e.g., "5"), if present
                    
                    if limit is None:  # If no limit is provided, default to 1
                        limit = 1
                    else:
                        limit = int(limit)  # Convert limit to an integer
                    
                    if limit == 1 or limit is None:
                        # Special case: Treat as "student with the highest GPA"
                        query = f"SELECT {FIELD_MAPPING.get('name', 'Name')}, {FIELD_MAPPING.get('score', 'GPA')} " \
                            f"FROM {FIELD_MAPPING.get('table_name', 'student_data')} " \
                            f"ORDER BY {FIELD_MAPPING.get('score', 'GPA')} DESC LIMIT 1;"

                        description = "This query retrieves the student with the highest GPA."
                    else:
                        # General case: Top N students
                        query = pattern_details["sql"].format(
                            name_field=FIELD_MAPPING.get("name", "Name"),
                            score_field=FIELD_MAPPING.get("score", "GPA"),
                            table_name=FIELD_MAPPING.get("table_name", "students"),
                            limit=limit
                        )
                        description = pattern_details["description"].replace("{limit}", str(limit))
                    
                    return query, description
                
            elif pattern_key == "students_count_by_gender":
                match = re.match(pattern_details["pattern"], user_input, re.IGNORECASE)
                if match:
                    # Check if specific gender (male/female) is mentioned
                    specific_gender = match.group(2) if len(match.groups()) > 1 else None
                    where_clause = ""
                    
                    if specific_gender:
                        # Add WHERE clause for specific gender
                        where_clause = f"WHERE {FIELD_MAPPING.get('gender_field', 'Gender')} = '{specific_gender.capitalize()}'"
                    
                    # Generate query
                    query = pattern_details["sql"].format(
                        gender_field=FIELD_MAPPING.get("gender_field", "Gender"),
                        table_name=FIELD_MAPPING.get("table_name", "students"),
                        where_clause=where_clause
                    )
                    
                    description = pattern_details["description"]
                    if specific_gender:
                        description += f" (filtering for {specific_gender} students)."
                    
                    return query, description
                
            elif pattern_key == "students_count_by_gender_and_category":
                match = re.match(pattern_details["pattern"], user_input, re.IGNORECASE)
                if match:
                    query_type = match.group(1)  # "how many" or "count of"
                    gender = match.group(2).capitalize()  # Male or Female
                    location = match.group(3)  # Location if provided
                    department = match.group(4)  # Department if provided
                    year = match.group(5)  # Year if provided

                    # Initialize conditions
                    location_condition = ""
                    department_condition = ""
                    year_condition = ""
                    category_field = FIELD_MAPPING.get("gender_field", "Gender")  # Default category field

                    # Normalize and add location condition
                    if location:
                        normalized_location = KNOWN_STORE_LOCATIONS.get(location.lower())
                        if normalized_location:
                            category_field = FIELD_MAPPING.get("store_location", "City")
                            location_condition = f"AND {category_field} = '{normalized_location}'"
                        else:
                            return None, f"Invalid location: {location}. Please provide a valid location."

                    # Add department condition
                    if department:
                        category_field = FIELD_MAPPING.get("department", "Department")
                        department_condition = f"AND {category_field} = '{department}'"

                    # Add year condition
                    if year:
                        category_field = FIELD_MAPPING.get("year", "Year")
                        year_condition = f"AND {category_field} = {year}"

                    # Generate SQL query
                    query = pattern_details["sql"].format(
                        category_field=category_field,
                        gender_field=FIELD_MAPPING.get("gender_field", "Gender"),
                        gender=gender,
                        location_condition=location_condition,
                        department_condition=department_condition,
                        year_condition=year_condition,
                        table_name=FIELD_MAPPING.get("table_name", "students")
                    )

                    # Generate dynamic description
                    filters = []
                    if location:
                        filters.append(f"location: {normalized_location}")
                    if department:
                        filters.append(f"department: {department}")
                    if year:
                        filters.append(f"year: {year}")

                    filters_text = ", ".join(filters) if filters else "no specific filters"
                    description = f"This query counts the number of {gender} students grouped by {category_field}, filtered by {filters_text}."
                    
                    return query, description

            elif pattern_key == "average_streams_by_artist":
                # Generate query using the field mapping
                query = pattern_details["sql"].format(
                    artist_field=FIELD_MAPPING.get("artist", "artist"),
                    streams_field=FIELD_MAPPING.get("streams", "streams"),
                )
                description = pattern_details["description"]
                return query, description


            elif pattern_key == "average_gpa_by_category":
                # Extract category (department or year) from user query using regex group
                match = re.match(pattern_details["pattern"], user_input, re.IGNORECASE)
                if match:
                    category = match.group(2)  # Capture 'department' or 'year' from the pattern
                    
                    # Use FIELD_MAPPING to get the correct database field
                    category_field = FIELD_MAPPING.get(category, category)  # Direct mapping from the user query
                    gpa_field = FIELD_MAPPING.get("gpa", "GPA")  # Map 'gpa' to its correct database field
                    
                    # Format the SQL query dynamically
                    query = pattern_details["sql"].format(
                        category_field=category_field,
                        gpa_field=gpa_field,
                    )
                    
                    # Extract the description
                    description = pattern_details["description"]
                    return query, description
                else:
                    return None, "The query pattern did not match for 'average_gpa_by_category'."

    # Default case when no patterns match
    return None, (
        "I couldn't understand your query. Try one of the following examples:\n"
        "- Total sales by category\n"
        "- Total revenue for the store in Manhattan\n"
        "- Top 5 best-selling products"
    )

              