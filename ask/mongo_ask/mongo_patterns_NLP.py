from ask.mongo_ask.mongo_helpers_NLP import gather_metrics
from ask.mongo_ask.mongo_globals_NLP import FIELD_MAPPING, KNOWN_STORE_LOCATIONS, PATTERNS
import re
from pprintpp import pprint
def generate_column_keywords(table_info):
    """Dynamically generate synonyms for each column."""
    column_keywords = {}

    # Common patterns for synonyms
    keyword_patterns = {
        "id": ["id", "identifier"],
        "category": ["category", "classification", "product category", "categories"],
        "product": ["product","products", "item", "model", "phone model", "products", "phones", "phone"],
        "type": ["type", "types","kind", "product type", "os", "os type", "os types"],
        "location": ["location", "place", "branch", "area", "store location", "store", "stores", "locations"],
        "date": ["date", "day", "time"],
        "name": ["name", "names", "title", "label"],
        "amount": ["amount", "value", "price", "cost"],
        "price": ["price", "cost", "price_usd"],
        "quantity": ["quantity", "quantities", "transaction_qty", "count", "number"],
        "phone brand": ["phone brand", "brand", "brands"],
        "model": ["phone model", "model", "models"],
        "song": ["song", "track", "songs", "tracks"],
        "track": ["song", "track", "songs", "tracks"],
        "name": ["artist", "singer", "singer names", "artist names"],
    }

    # Iterate through table_info to map columns dynamically
    for column in table_info["numeric"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)

    for column in table_info["categorical"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)

    for column in table_info["date"]:
        column_keywords[column] = generate_keywords(column, keyword_patterns)
    
    for column in table_info["others"]:
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


def initialize_patterns(db, collection_name, table_info):
    """Dynamically initialize patterns based on MongoDB collection schema."""
    global FIELD_MAPPING, PATTERNS, KNOWN_STORE_LOCATIONS

    # Step 1: Gather collection metrics
    # table_info = gather_metrics(db, collection_name)

    # Step 2: Generate column keywords and populate FIELD_MAPPING
    column_keywords = generate_column_keywords(table_info)
    for column, synonyms in column_keywords.items():
        for synonym in synonyms:
            FIELD_MAPPING[synonym] = column  # Map each synonym to its corresponding column

    # REMOVE
    print(FIELD_MAPPING)
    print(KNOWN_STORE_LOCATIONS)
    # Debug: Ensure FIELD_MAPPING is populated before further usage
    # print("Debug: FIELD_MAPPING after synonym population:", FIELD_MAPPING)

    # Step 3: Fetch dynamic fields (AFTER FIELD_MAPPING is populated)
    quantity_field = FIELD_MAPPING.get("quantity", 1)  # Default to 1 if not found
    price_field = FIELD_MAPPING.get("price", "price_usd")  # Default to 'price_usd' if not found
    product_field = FIELD_MAPPING.get("product", "product")
    location_field = FIELD_MAPPING.get("location", "store")
    date_field = FIELD_MAPPING.get("date", "launch_date")
    stream_field = FIELD_MAPPING.get("stream", "streams")
    name_field = FIELD_MAPPING.get("name", "artist_name")
    # Debug the fetched fields
    print(f"Quantity Field: {quantity_field}, Price Field: {price_field}, Product Field: {product_field}, Location Field: {location_field}, Date Field: {date_field}")

    # Define static patterns dynamically using the extracted fields
    static_patterns = {

        # TODO: 1. count_by_category
        
        #      4. average of entire dataset      /    IF you get one, you can replace for the others
        #      5. total sales of entire dataset /
        #      6. total sales by date and date_range
        #      7. disinct values of a column

        "total_sales_by_field": {
            "pattern": r".*total (sales|revenue) (by|for each|for every) (\w+)",
            "mongodb": [
                {
                    "$group": {
                        "_id": "${GROUP_FIELD}",
                        "total_sales": {
                            "$sum": {
                                "$multiply": [
                                    f"${quantity_field}" if quantity_field != 1 else 1,
                                    f"${price_field}"
                                ]
                            }
                        }
                    }
                }
            ],
            "description": "This query retrieves the total sales amount for each {field}."
        },
       "total_sales_by_location": {
    "pattern": r".*total (sales|revenue) (by|for each|for every|of each) (store location|location|store)",
    "mongodb": [
        {
            "$group": {
                "_id": f"${FIELD_MAPPING.get('location', 'store')}",  # Dynamically resolve 'location'
                "total_sales": {
                    "$sum": {
                        "$multiply": [
                            f"${FIELD_MAPPING.get('quantity', 1)}" if FIELD_MAPPING.get('quantity', 1) != 1 else "1",
                            f"${FIELD_MAPPING.get('price', 'price_usd')}"
                        ]
                    }
                }
            }
        }
    ],
    "description": "This query retrieves the total sales amount for each store location."
},

         "total_sales_by_date": {
    "pattern": r".*total sales (in|on|for|during) ([\w\s,]+)",
    "mongodb": {
        "specific_date": [
            {"$match": {"date_field": "specific_date"}},  # Replace date_field and specific_date dynamically
            {"$group": {
                "_id": None,
                "total_sales": {
                    "$sum": {"$multiply": [
                            f"${FIELD_MAPPING.get('quantity', 1)}" if FIELD_MAPPING.get('quantity', 1) != 1 else "1",
                            f"${FIELD_MAPPING.get('price', 'price_usd')}"]}  # Replace dynamically
                }
            }}
        ],
        "month": [
            {"$match": {
                "$expr": {"$eq": [{"$month": "$date_field"}, "month"]}  # Replace dynamically
            }},
            {"$group": {
                "_id": None,
                "total_sales": {
                    "$sum": {"$multiply": ["$quantity_field", "$price_field"]}  # Replace dynamically
                }
            }}
        ],
        "year": [
           {
        "$match": {
            "$expr": {
                "$eq": [
                    { "$year": "$transaction_date" },
                    2023
                ]
            }
        }
    },
            {
        "$group": {
            "_id": None,
            "total_sales": {
                "$sum": {
                    "$multiply": ["$transaction_qty", "$unit_price"]
                }
            }
        }
    },
        ]
    },
    "description": "This query retrieves the total sales for a specific date, month, or year."
},



        "top_best_selling_products": {
            "pattern": r".*top (\d+)\s+(?:best[-\s]?selling)\s+(products|models|items|phones|phone models|brands|os types)",
            "mongodb": [
                {"$group": {
                    "_id": "${GROUP_FIELD}",  # Replace dynamically
                    "total_quantity": {"$sum": f"${quantity_field}" if quantity_field != 1 else 1,}  # Replace dynamically
                }},
                {"$sort": {"total_quantity": -1}},
                {"$limit": "{LIMIT}"}  # Replace with the limit dynamically
            ],
            "description": "This query retrieves the top {limit} best-selling products or models by quantity."
        },
        
        # TODO: DONE"
        "top_least_selling_products": {
            "pattern": r".*top (\d+)\s+(?:least|worst|least[-\s]?selling|worst[-\s]?selling)\s+(products|models|items|phones|phone models|brands|os types)",
            "mongodb": [
                {"$group": {
                    "_id": "${GROUP_FIELD}",  # Replace dynamically
                    "total_quantity": {"$sum": f"${quantity_field}" if quantity_field != 1 else 1,}  # Replace dynamically
                }},
                {"$sort": {"total_quantity": 1}},
                {"$limit": "{LIMIT}"}  # Replace with the limit dynamically
            ],
            "description": "This query retrieves the top {limit} best-selling products or models by quantity."
        },

         # TODO: FIX THE REGEX SO THAT IT MATCHES "WORST" AND "LEAST"
        "top_least_streamed_songs": {
            "pattern": r"(top (\d+)\s+(?:(least|worst|least)[-\s]?streamed)\s+(tracks|songs))|((song|track)\s+with\s+lowest\s+streams)",
            "mongodb": [
                {"$group": {
                    "_id": "${GROUP_FIELD}",  # Replace dynamically
                    "total_streams": {"$sum": f"${stream_field}"}  # Replace dynamically
                }},
                {"$sort": {"total_streams": 1}},
                {"$limit": "{LIMIT}"}  # Replace with the limit dynamically
            ],
            "description": "This query retrieves the top {limit} most-streamed tracks or songs."
        },


        "top_most_streamed_songs": {
            "pattern": r"(top (\d+)\s+(?:(most|highest)[-\s]?streamed)\s+(tracks|songs|song|artist))|((song|track|artist)\s+with\s+highest\s+streams)",
            "mongodb": [
                {"$group": {
                    "_id": "${GROUP_FIELD}",  # Replace dynamically
                    "total_streams": {"$sum": f"${stream_field}"}  # Replace dynamically
                }},
                {"$sort": {"total_streams": -1}},
                {"$limit": "{LIMIT}"}  # Replace with the limit dynamically
            ],
            "description": "This query retrieves the top {limit} most-streamed tracks or songs."
        },

        "specific_product_sales": {
            "pattern": r".*sales of (.+)",
            "mongodb": [ # Replace dynamically  
                
                {"$group": {
                    "_id": "${GROUP_FIELD}",
                    "total_quantity": {"$sum":  f"${quantity_field}" if quantity_field != 1 else 1,},  # Replace dynamically
                    "total_sales": {"$sum": {"$multiply": [f"${quantity_field}" if quantity_field != 1 else 1, f"${price_field}"]}}  # Replace dynamically
                }}
            ],
            "description": "This query retrieves the total quantity and sales of a specific product."
        },

        # --------------pattern works----------------

        # "total_sales_by_store": {
        #     "pattern": r".*total (revenue|sales) (for the store in|in) (\w+)",
        #     "mongodb": [
        #         {"$match": f"${location_field}"},  # Replace dynamically
        #         {"$group": {
        #             "_id": f"${location_field}",
        #             "total_sales": {"$sum": {"$multiply": [f"${quantity_field}" if quantity_field != 1 else "1", 
        #                                                    f"${FIELD_MAPPING.get('price', 'price_usd')}"]}}  # Replace dynamically
        #         }}
        #     ],
        #     "description": "This query retrieves the total revenue for the store in {store}."
        # },


        # "quantity_by_category_in_location": {
        #     "pattern": r".*quantity of products sold by category in (\w+(?:'?\w+)?)",
        #     "mongodb": [
        #         {"$match": {"store_location": "location"}},  # Replace dynamically
        #         {"$group": {
        #             "_id": "$product_category",  # Replace dynamically
        #             "total_quantity": {"$sum": "$quantity_field"}  # Replace dynamically
        #         }}
        #     ],
        #     "description": "This query retrieves the quantity of products sold by category in {location}."
        # },

# "quantity_by_category": {
#     "pattern": r".*quantity of (\w+(?:'?\w+)?)",
#     "mongodb": [
#         {"$group": {
#             "_id": "${GROUP_FIELD}",  # Replace dynamically
#             "total_quantity": {"$sum": f"${quantity_field}"},  # Replace dynamically
#             "total_sales": {"$sum": f"${quantity_field}"},  # Replace dynamically
#             "product_names": {"$addToSet": "${name_field}"}  # Collect distinct product names
#         }}
#     ],
#     "description": "This query retrieves the total quantity of products sold in {location}, along with distinct counts and projections."
# },

        "quantity_by_category": {
            "pattern": r".*quantity of (\w+(?:'?\w+)?)",
            "mongodb": [
                {
                    "$group": {
                        "_id": "${GROUP_FIELD}",  # Replace dynamically with the category field
                        "total_quantity": {"$sum": f"${quantity_field}"},  # Replace dynamically
                        "total_sales": {"$sum": f"${quantity_field}"},  # Replace dynamically
                        "product_names": {"$addToSet": "${name_field}"},  # Collect distinct product names
                        "total_categories": {"$sum": 1}  # Count occurrences of categories
                    }
                }
            ],
            "description": "This query retrieves the total quantity of products sold in {location}, along with category counts and product details."
        },

#------------------------------- COUNT
        "simple_count": {
            "pattern": r".*count of (\w+(?:'?\w+)?)",
            "mongodb":         
                [
                {
                    "$group": {
                        "_id":"${GROUP_FIELD}",  # Group by the field "product"
                        "count": {"$sum": 1}  # Count occurrences of each product
                    }
                },
                {
                    "$sort": {"count": -1}  # Sort by count in descending order (optional)
                }
            ],
            "description": "This query retrieves the total quantity of products sold in {location}, along with category counts and product details."
        },

# -------------------------------LIST
        "simple_list": {
            "pattern": r".*list of (\w+(?:'?\w+)?)",
            "mongodb":         
[
    # Stage 1: Group by the specified field
    {
        "$group": {
            "_id": "${GROUP_FIELD}",  # Replace with the field name dynamically
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
            "_id": 0,  # Add title dynamically
            "total_count": 1,
            "list": 1
        }
    }
],
            "description": "This query retrieves the total quantity of products sold in {location}, along with category counts and product details."
        },

# -------------------------------FIND

        "simple_find": {
            "pattern": r".*find (\w+(?:'?\w+)?) where (\w+(?:'?\w+)?) is (\w+(?:'?\w+)?)",
            "mongodb": [
                {
                    "$match": {"${FILTER_FIELD}": "${FILTER_VALUE}"}  # Dynamic filter placeholder
                },
                {
                    "$project": {"${FIELD_TO_RETURN}": 1, "_id": 0}  # Dynamic projection placeholder
                }
            ],
            "description": "This query retrieves the {FIELD_TO_RETURN} where {FILTER_FIELD} is {FILTER_VALUE}."
        },


        "average_price_by_field": {
            "pattern": r".*average price (of|for|for each|by) (\w+)",  # Multi-word support
            "mongodb": [
                {
                    "$group": {
                    "_id": f"${FIELD_MAPPING.get('field', '_id')}",  # Group by phone_model
                    "avg_price": { "$avg": f"${price_field}" }  # Calculate average of price_usd
                    }
                }
                ],
            "description": "This query calculates the average price for each {field}."
        },

        "most_expensive": {
            "pattern": r".*most expensive (phone|product|item|model)",
            "mongodb": [
                {"$sort": {FIELD_MAPPING.get('price', 'price'): -1}},  # Replace dynamically
                {"$limit": 1},
                {"$project": {FIELD_MAPPING.get('product', 'product'): 1, "_id": 0}}
            ],
            "description": "This query retrieves the most expensive product."
        },

        "maximum_value": {
            "pattern": r".*maximum value of (\w+)",
            "mongodb": [
                     {
                        "$sort": { "{FIELD_NAME}": -1 }  #// Sort in descending order by the field
                    },
                    {
                        "$limit": 1 # // Limit the result to the top document
                    },
                    {"$project": {f"{FIELD_MAPPING.get('product', "product")}": 1, "{FIELD_NAME}": 1, "_id": 0}}
            ],
            "description": "This query retrieves the most expensive product."
        },

        "minimum_value": {
            "pattern": r".*minimum value of (\w+)",
            "mongodb": [
                     {
                        "$sort": { "{FIELD_NAME}": 1 }  #// Sort in ascending order by the field
                    },
                    {
                        "$limit": 1 # // Limit the result to the top document
                    },
                    {"$project": {f"{FIELD_MAPPING.get('product', "product")}": 1, "{FIELD_NAME}": 1, "_id": 0}}
            ],
            "description": "This query retrieves the most expensive product."
        },


        "average_value": {
            "pattern": r".*average value of (\w+)",
            "mongodb": [
                {
                    "$group": {
                        "_id": None,  # No grouping, calculate average for the entire collection
                        "average_value": { "$avg": "${FIELD_NAME}" }  # Dynamically replace {FIELD_NAME}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "average_value": 1  # Include only the average value in the output
                    }
                }
            ],
            "description": "This query calculates the average value of the specified field."
        },


        "least_expensive": {
            "pattern": r".*least expensive (phone|product|item|model)",
            "mongodb": [
                {"$sort": {FIELD_MAPPING.get("price", "price"): 1}},  # Replace dynamically
                {"$limit": 1},
                {"$project": {FIELD_MAPPING.get('product', 'product'): 1, "_id": 0}}
            ],
            "description": "This query retrieves the least expensive product."
        },

    #     "most_streamed_artist": {
    #         "pattern": r".*(most streamed|highest streamed) artist",
    #         "mongodb": [
    #             {"$group": {
    #                 "_id": f"${name_field}",  # Replace dynamically
    #                 "total_streams": {"$sum": f"${stream_field}"}  # Replace dynamically
    #             }},
    #             {"$sort": {"total_streams": -1}},
    #             {"$limit": 1}
    #         ],
    #         "description": "This query retrieves the artist with the highest total streams."
    #     }
    }

    # Debug: Static patterns before updates
    # print("Debug: Static Patterns before dynamic updates:", static_patterns)

    # Step 4: Update PATTERNS dynamically
    # updated_patterns = {}
    # for pattern_key, pattern_details in static_patterns.items():
    #     mongodb_query = pattern_details.get("mongodb", [])
    #     updated_mongodb = []
    #     for stage in mongodb_query:
    #         if "$group" in stage:
    #             group_stage = stage["$group"]
    #             # group_stage["_id"] = f"${FIELD_MAPPING.get(group_stage.get('_id', '').replace('field', ''), '_id')}"
    #             group_stage["total_sales"] = {
    #                 "$sum": {
    #                     "$multiply": [
    #                         f"${FIELD_MAPPING.get('quantity', 1)}",
    #                         f"${FIELD_MAPPING.get('price', 'price_usd')}"
    #                     ]
    #                 }
    #             }
    #         updated_mongodb.append(stage)
    #     updated_patterns[pattern_key] = {**pattern_details, "mongodb": updated_mongodb}

    # Update PATTERNS globally
    PATTERNS.update(static_patterns)

    # Debug: Final PATTERNS
    # print("Debug: Final PATTERNS with MongoDB queries initialized.")
    # print("FIELD_MAPPING:", FIELD_MAPPING)
    # print("KNOWN_STORE_LOCATIONS:", KNOWN_STORE_LOCATIONS)

