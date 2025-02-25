from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Response, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import insert, select, func, extract, and_
from pymysql.err import MySQLError
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql
from typing import List, Optional
from databases import Database
from datetime import datetime, timedelta
from elasticsearch import helpers
import time
import os
import json
import jwt
import asyncio

from ..config import get_database, get_redis, get_elasticsearch_async
from ..models import model_description, model_app, model_developer, model_category_apps__model_app_categories, model_category, model_sdkversion
from ..models import model_name, model_download, model_version, model_androidmanifest, model_app_permissions, model_permissionrequested, model_rating
from ..models import download_log
from ..schemas import AppResults, AppDetails, VersionDetails, QueryParams
from .user_routes import get_current_user
from ..env import SECRET_KEY, ALGORITHM, INDEX

MATURITY = ["Everyone", "Low Maturity", "Medium Maturity", "High Maturity"]
ACCESS_TOKEN_EXPIRE_MINUTES = 5  # validity for pre-signed url
cache_expiration = 43200

router = APIRouter()

# async def get_query_params(
#     query: Optional[str] = None,
#     package_name: Optional[str] = None,
#     developer_name: Optional[str] = None,
#     categories: Optional[str] = None,
#     maturity: Optional[str] = None,
#     downloadable: Optional[bool] = True,
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
# ):
#     # if not any([query, package_name, developer_name, categories]):
#     #     raise HTTPException(status_code=400, detail="At least one of 'query', 'package_name', 'developer_name', 'categories' must be provided.")
#     return {
#         "query": query,
#         "package_name": package_name,
#         "developer_name": developer_name,
#         "categories": categories,
#         "maturity": maturity,
#         "downloadable": downloadable,
#         "page": page,
#         "limit": limit
#     }

# @router.get("/search/", response_model=List[AppResults])
# async def search_apps(
#     response: Response,
#     params: dict = Depends(get_query_params),
#     database: Database = Depends(get_database)
# ):
#     offset = (params["page"] - 1) * params["limit"]
#     name_query = f"%{params['query']}%" if params["query"] else None
#     package_query = f"%{params['package_name']}%" if params["package_name"] else None
#     developer_query = f"%{params['developer_name']}%" if params["developer_name"] else None
#     category_query = params['categories'].split(",") if params["categories"] else None
#     maturity_query = params['maturity'].split(",") if params["maturity"] else None
#     print('Searching initiated ...', params)

#     # Aliases for the tables
#     ma = model_app.alias("ma")
#     mdev = model_developer.alias("mdev")
#     mca = model_category_apps__model_app_categories.alias("mca")
#     mc = model_category.alias("mc")
#     mn = model_name.alias("mn")
#     dl = model_download.alias("dl")
    
#     base_query = (
#         select(
#             mn.c.id,
#             mn.c.name,
#             mn.c.created_on,
#             mn.c.app_id,
#             ma.c.app_id.label("package_name"),
#             func.row_number().over(partition_by=mn.c.app_id, order_by=mn.c.created_on.desc()).label("row_num")
#         ).select_from(mn.join(ma, mn.c.app_id == ma.c.id))
#     )

#     # Apply filters based on provided parameters
#     filters = []
#     if name_query:
#         filters.append(mn.c.name.like(name_query))
#     if package_query:
#         filters.append(ma.c.app_id.like(package_query))
#     if developer_query:
#         filters.append(mdev.c.developer_id.like(developer_query))
#         base_query = base_query.join(mdev, ma.c.developer_id == mdev.c.id)
#     category_query = category_query + maturity_query if category_query and maturity_query else (maturity_query or category_query)
#     if category_query:
#         subquery = (
#             select(mca.c.model_app_id)
#             .select_from(mca.join(mc, mca.c.model_category_id == mc.c.id))
#             .where(mc.c.name.in_(category_query))
#             .group_by(mca.c.model_app_id)
#             .having(func.count(mca.c.model_app_id) >= len(category_query))
#         ).alias("subquery")
        
#         base_query = base_query.join(subquery, ma.c.id == subquery.c.model_app_id)
        

#     if params["downloadable"]:
#         base_query = base_query.join(dl, ma.c.id == dl.c.app_id)

#     if filters:
#         base_query = base_query.where(and_(*filters))

    
#     # Apply row number filter to get the latest entry for each app_id
#     filtered_query = base_query.alias("subquery")
#     row_num_filtered_query = select(
#         filtered_query.c.id,
#         filtered_query.c.name,
#         filtered_query.c.created_on,
#         filtered_query.c.app_id,
#         filtered_query.c.package_name
#     ).where(filtered_query.c.row_num == 1)

#     # Count subquery
#     count_stmt = select(func.count().label("total")).select_from(row_num_filtered_query.alias("count_subquery"))

#     # Apply offset and limit
#     query_stmt = row_num_filtered_query.offset(offset).limit(params["limit"])

#     start_time = time.time()
#     # Check if the total count for this query is cached
#     cache_params = params.copy()
#     cache_result_key = f"result:{cache_params}"
#     redis_client = get_redis()
#     results = await redis_client.get(cache_result_key)
    
#     start_time = time.time()
#     if results is None:
#         db_results = await database.fetch_all(query_stmt)
#         results = [serialize_result(result) for result in db_results]
#         await redis_client.set(cache_result_key, json.dumps(results), ex=36000)  
#     else:
#         results = json.loads(results)
#         await redis_client.expire(cache_result_key, 36000)
#     main_query_time = time.time() - start_time
#     print(f"Main query time: {main_query_time:.2f} seconds")

#     cache_params.pop("page", None)
#     cache_params.pop("limit", None)
#     cache_count_key = f"count:{cache_params}"
#     total_count = await redis_client.get(cache_count_key)
#     start_time = time.time()
#     if total_count is None:
#         total_count = await database.fetch_val(count_stmt)
#         await redis_client.set(cache_count_key, total_count, ex=36000)  
#     else:
#         total_count = int(total_count)
#         await redis_client.expire(cache_count_key, 36000)
#     count_query_time = time.time() - start_time
#     print(f"Count query time: {count_query_time:.2f} seconds")

#     # Print the query in raw SQL format
#     print(query_stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

#     if not results:
#         raise HTTPException(status_code=404, detail="No matching records found")

#     response.headers['x-total-count'] = str(total_count)
#     return results

async def get_query_params(
    keyword: Optional[str] = None,
    query: Optional[str] = None,
    package_name: Optional[str] = None,
    developer_name: Optional[str] = None,
    categories: Optional[str] = None,
    maturity: Optional[str] = None,
    permissions: Optional[str] = None,
    downloadable: Optional[bool] = True,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    try:
        # Validate parameters with Pydantic
        params = QueryParams(
            keyword=keyword,
            query=query,
            package_name=package_name,
            developer_name=developer_name,
            categories=categories,
            maturity=maturity,
            permissions=permissions,
            downloadable=downloadable,
            page=page,
            limit=limit,
        )
        return params.dict()
    except ValidationError as e:
        print('Validation error in search req')
        raise HTTPException(status_code=400, detail=f"Input validation error: {e.errors()}")
    
@router.get("/search/", response_model=List[dict])
async def search_apps(
    response: Response,
    params: dict = Depends(get_query_params),
    es_client = Depends(get_elasticsearch_async),
    redis_client=Depends(get_redis)
):
    offset = (params["page"] - 1) * params["limit"]

    cache_key = f"search:{hash(frozenset(params.items()))}:{offset}:{params['limit']}"
    
    print(cache_key)

    try:
        # Check if the query result is cached
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            # Load cached result and set headers
            cached_data = json.loads(cached_result)
            await redis_client.expire(cache_key, cache_expiration)
            response.headers["x-total-count"] = str(cached_data["total_count"])
            return cached_data["hits"]
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve cached results: {e}")

    
    # Combine categories and maturity terms
    category_maturity_terms = []
    if params["categories"]:
        category_maturity_terms += params["categories"].split(",")
    if params["maturity"]:
        category_maturity_terms += params["maturity"].split(",")

    # Construct the Elasticsearch query
    es_query = {"bool": {"must": [], "should": [], "filter": []}}

    # Keyword search across multiple fields (OR logic internally)
    if params["keyword"]:
        es_query["bool"]["should"].extend([
            # Search in names
            {
                "nested": {
                    "path": "names",
                    "query": {
                        "match": {
                            "names.name": {
                                "query": params["keyword"],
                                "operator": "AND",
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                }
            },
            # Search in descriptions
            {
                "nested": {
                    "path": "descriptions",
                    "query": {
                        "match": {
                            "descriptions.description": {
                                "query": params["keyword"],
                                "operator": "AND",
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                }
            },
            # Search in package_name using raw field for exact or wildcard search
            {
                "wildcard": {
                    "package_name.raw": {
                        "value": f"*{params['keyword']}*",  # Substring match
                        "case_insensitive": True
                    }
                }
            },
            # Search in developer_name for partial matches
            {
                "match": {
                    "developer_name": {
                        "query": params["keyword"],
                        "operator": "AND",
                        "fuzziness": "AUTO"
                    }
                }
            },
            # Search in categories for exact matches
            {
                "term": {
                    "categories": {
                        "value": params["keyword"]
                    }
                }
            },
            # Search in versions.permissions using raw field for exact or wildcard match
            {
                "nested": {
                    "path": "versions",
                    "query": {
                        "wildcard": {
                            "versions.permissions.raw": {
                                "value": f"*{params['keyword']}*",  # Substring match
                                "case_insensitive": True
                            }
                        }
                    }
                }
            }
        ])
        es_query["bool"]["minimum_should_match"] = 1  # At least one of the should clauses must match

    if params["query"]:
        es_query["bool"]["must"].append({
            "nested": {
                "path": "names",
                "query": {
                    "match": {
                        "names.name": {
                            "query": params["query"],
                            "operator": "AND",
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        })


    if params.get("package_name"):
        es_query["bool"]["must"].append({
            "wildcard": {
                "package_name.raw": {
                    "value": f"*{params['package_name']}*",  # Match substring
                    "case_insensitive": True  # Make it case-insensitive
                }
            }
        })
    
    if params["developer_name"]:
        es_query["bool"]["must"].append({
            "match_phrase": {
                "developer_name": params["developer_name"]
            }
        })
    
    if category_maturity_terms:
        # Ensure all specified categories/maturity terms match exactly
        es_query["bool"]["must"].append({
            "terms_set": {
                "categories": {
                    "terms": category_maturity_terms,
                    "minimum_should_match_script": {
                        "source": "params.num_terms"
                    }
                }
            }
        })

    if params["permissions"]:
        permissions_terms = params["permissions"].split(",")
        es_query["bool"]["must"].append({
            "nested": {
                "path": "versions",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "wildcard": {
                                    "versions.permissions.raw": {
                                        "value": f"*{perm.strip()}*",
                                        "case_insensitive": True
                                    }
                                }
                            } for perm in permissions_terms
                        ]
                    }
                }
            }
        })

    # Check if the app is downloadable (has at least one version)
    if params["downloadable"]:
        es_query["bool"]["filter"].append({
            "nested": {
                "path": "versions",
                "query": {"exists": {"field": "versions"}}
            }
        })

    print(es_query)
    try:
        # Step 1: Use _count endpoint to get total count
        count_response = await es_client.count(
            index=INDEX,
            body={"query": es_query}
        )
        total_count = count_response["count"]
        print(total_count)
        
        # Step 2: Retrieve paginated search results
        es_response = await es_client.search(
            index=INDEX,
            body={
                "size": params["limit"],
                "from": offset,
                "query": es_query
            }
        )
        
        # Extract and process hits
        hits = []
        for i, hit in enumerate(es_response["hits"]["hits"], start=1 + offset):
            source = hit["_source"]

            # Find the latest name based on `created_on`
            latest_name_entry = max(source.get("names", []), key=lambda x: x["created_on"], default={})
            latest_name = latest_name_entry.get("name", "")

            # Construct the filtered document
            hits.append({
                "id": i,  # Incremental ID based on the document's position
                "app_id": source.get("app_id"),
                "name": latest_name,
                "package_name": source.get("package_name")
            })

        # Set response headers
        if total_count >= 50000:
            total_count = 50000 # elastic search limitation, solution -> scroll api or config
            
        # Cache the result in Redis with a 6 hr expiration time
        await redis_client.set(
            cache_key,
            json.dumps({"total_count": total_count, "hits": hits}),
            ex=cache_expiration  # Expiration time in seconds 
        )
        
        response.headers["x-total-count"] = str(total_count)
        return hits

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")

def serialize_result(result):
    # Convert Record to dictionary
    result_dict = dict(result)
    # Convert datetime objects to string format
    for key, value in result_dict.items():
        if isinstance(value, datetime):
            result_dict[key] = value.isoformat()  # Convert datetime to ISO format string
    return result_dict

@router.get("/details/{app_id}", response_model=AppDetails)
async def fetchDetails(
    app_id: int, 
    database: Database = Depends(get_database), 
    redis_client = Depends(get_redis)  # Redis client for caching
):
    if app_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid app_id")
    # Generate a unique cache key for the app_id
    cache_key = f"details:{app_id}"

    try:
        # Check if the details are cached
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            # Return the cached result if available
            await redis_client.expire(cache_key, cache_expiration)
            return json.loads(cached_result)

        # Query database for app details
        md = model_description.alias("md")
        ma = model_app.alias("ma")
        mdev = model_developer.alias("mdev")
        mca = model_category_apps__model_app_categories.alias("mca")
        mc = model_category.alias("mc")
        mn = model_name.alias("mn")

        query_stmt = (
            select(
                md.c.id,
                mn.c.name,
                md.c.text,
                md.c.created_on,
                md.c.app_id,
                mdev.c.developer_id,
                ma.c.app_id.label("package_name"),
                func.group_concat(mc.c.name).label("categories")
            )
            .select_from(
                md.join(ma, md.c.app_id == ma.c.id)
                .join(mdev, ma.c.developer_id == mdev.c.id)
                .join(mca, ma.c.id == mca.c.model_app_id)
                .join(mc, mca.c.model_category_id == mc.c.id)
                .join(mn, md.c.app_id == mn.c.app_id)
            )
            .where(md.c.app_id == app_id)
            .group_by(md.c.id, md.c.text, md.c.created_on, md.c.app_id, mdev.c.developer_id, mn.c.name, ma.c.app_id)
        )
        
        result = await database.fetch_one(query_stmt)
        
        if not result:
            raise HTTPException(status_code=404, detail="Details not found")
        
        # Process the result
        result = serialize_result(result)
        result["categories"] = result["categories"].split(",") if result["categories"] else []
        result["maturity"] = [category for category in result["categories"] if category in MATURITY]
        result["categories"] = [category for category in result["categories"] if category not in MATURITY]

        # Cache the result in Redis with an expiration time (e.g., 1 hour)
        await redis_client.set(cache_key, json.dumps(result), ex=cache_expiration)

        return result

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Fetching details failed: {e}")

@router.get("/version-details/{app_id}", response_model=List[VersionDetails])
async def get_version_details(
    app_id: int, 
    database: Database = Depends(get_database), 
    redis_client = Depends(get_redis)  # Redis client for caching
):
    if app_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid app_id")
    # Generate a unique cache key for the app_id
    cache_key = f"version-details:{app_id}"

    try:
        # Check if the version details are cached
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            # Return the cached result if available
            await redis_client.expire(cache_key, cache_expiration)
            return json.loads(cached_result)

        # Aliases for the tables
        md = model_download.alias("md")
        mv = model_version.alias("mv")
        permissions_1 = model_app_permissions.alias("permissions_1")
        permissions_2 = model_permissionrequested.alias("permissions_2")
        ma = model_androidmanifest.alias("ma")
        mr = model_rating.alias("mr")
        sdk = model_sdkversion.alias("sdk")

        # Construct the query
        query_stmt = (
            select(
                md.c.id,
                md.c.hash, 
                md.c.size, 
                md.c.created_on, 
                mv.c.version, 
                permissions_1.c.name.label("permission_1"),
                permissions_2.c.name.label("permission_2"),
                mr.c.number_of_ratings,
                mr.c.one_star_ratings,
                mr.c.two_star_ratings,
                mr.c.three_star_ratings,
                mr.c.four_star_ratings,
                mr.c.five_star_ratings,
                sdk.c.min_sdk_number,
                sdk.c.target_sdk_number
            )
            .select_from(
                md
                .join(mv, md.c.version_id == mv.c.id)
                .outerjoin(permissions_1, md.c.id == permissions_1.c.download_id)
                .outerjoin(ma, md.c.id == ma.c.download_id)
                .outerjoin(permissions_2, ma.c.id == permissions_2.c.manifest_id)
                .outerjoin(sdk, ma.c.id == sdk.c.manifest_id)
                .outerjoin(
                    mr,
                    (md.c.app_id == mr.c.app_id) &
                    (extract('year', md.c.created_on) == extract('year', mr.c.created_on))
                )
            )
            .where(md.c.app_id == app_id)
            .order_by(md.c.created_on.desc()) 
        )

        # Execute the query
        results = await database.fetch_all(query_stmt)
        
        if not results:
            raise HTTPException(status_code=404, detail="No matching records found")

        download_details = {}
        
        for row in results:
            download_id = row["id"]
            hash_value = row["hash"]
            size = row["size"]
            created_on = row["created_on"]
            version = row["version"]
            permission_1 = row["permission_1"]
            permission_2 = row["permission_2"]
            number_of_ratings = row["number_of_ratings"]
            one_star_ratings = row["one_star_ratings"]
            two_star_ratings = row["two_star_ratings"]
            three_star_ratings = row["three_star_ratings"]
            four_star_ratings = row["four_star_ratings"]
            five_star_ratings = row["five_star_ratings"]
            min_sdk = row["min_sdk_number"]
            target_sdk = row["target_sdk_number"]
            
            if download_id not in download_details:
                download_details[download_id] = {
                    "hash": hash_value,
                    "size": size,
                    "created_on": created_on,
                    "version": version,
                    "permissions": set(),
                    "total_ratings": number_of_ratings if number_of_ratings is not None else 0,
                    "rating": round(sum((i + 1) * r for i, r in enumerate([one_star_ratings, two_star_ratings, three_star_ratings, four_star_ratings, five_star_ratings])) / number_of_ratings, 2) if number_of_ratings is not None and number_of_ratings != 0 else 0,
                    "min_sdk": min_sdk if min_sdk is not None and 1 <= min_sdk <= 35 else None,
                    "target_sdk": target_sdk if target_sdk is not None and 1 <= target_sdk <= 35 and target_sdk >= min_sdk else None,
                }
            
            if permission_1:
                download_details[download_id]["permissions"].add(permission_1)
            if permission_2:
                download_details[download_id]["permissions"].add(permission_2)

        version_details = [
            VersionDetails(
                hash=details["hash"],
                size=details["size"],
                version=details["version"],
                created_on=details["created_on"],
                permissions=list(details["permissions"]),
                total_ratings=details["total_ratings"],
                rating=details["rating"],
                min_sdk=details["min_sdk"],
                target_sdk=details["target_sdk"]
            )
            for details in download_details.values()
        ]

        serialized_data = [serialize_result(detail.dict()) for detail in version_details]
        await redis_client.set(cache_key, json.dumps(serialized_data), ex=cache_expiration)

        return version_details

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Fetching version details failed: {e}")

@router.get("/categories", response_model=List[str])
async def get_categories(database: Database = Depends(get_database)):
    redis_client = get_redis()
    cache_key = "categories"

    # Try to get the categories from Redis cache
    cached_categories = await redis_client.get(cache_key)
    
    if cached_categories:
        # If categories are found in cache, return them
        return json.loads(cached_categories)

    # If not found in cache, fetch from database
    query = select(model_category.c.name)
    results = await database.fetch_all(query)
    
    if not results:
        raise HTTPException(status_code=404, detail="No categories found")

    unique_categories = list(set([result['name'] for result in results]))
    categories = [category for category in unique_categories if category not in MATURITY]

    await redis_client.set(cache_key, json.dumps(categories))

    return categories

@router.get("/maturity", response_model=List[str])
async def get_maturity():
    return MATURITY

# Base directories to search for the file
ALLOWED_BASE_DIRS = [
    "/Volumes/apks",
    "/Volumes/apks/2018",
    "/Volumes/apks/old_apks"
]

def sanitize_file_path(hash_value: str) -> str:
    """
    Sanitize the hash_value to ensure it cannot traverse directories.
    """
    return os.path.basename(hash_value)  # Ensure only the base name is used


def validate_file_path(file_path: str) -> bool:
    """
    Ensure the file path is within the allowed base directories.
    """
    for base_dir in ALLOWED_BASE_DIRS:
        if os.path.commonpath([base_dir, file_path]) == base_dir:
            return True
    return False

def find_file_path(hash_value: str) -> str:
    """
    Search for the file in the allowed directories and validate the path.
    """
    # Sanitize the hash_value
    sanitized_hash = sanitize_file_path(hash_value)

    # Construct possible paths and validate
    for base_dir in ALLOWED_BASE_DIRS:
        file_path = os.path.join(
            base_dir,
            sanitized_hash[:1],
            sanitized_hash[1:2],
            sanitized_hash[2:3],
            sanitized_hash[3:4],
            sanitized_hash[4:5],
            sanitized_hash[5:6],
            sanitized_hash
        )
        if validate_file_path(file_path) and os.path.exists(file_path):
            return file_path
    raise FileNotFoundError("File not found.")

async def get_package_name(hash_value: str, database) -> str:
    """
    Fetches the package name associated with a given hash_value by performing a single query
    that joins model_download and model_app.

    Parameters:
    - hash_value (str): The hash value to lookup in the model_download.
    - database: The database connection dependency from get_database.

    Returns:
    - str: The package name associated with the hash_value.
    """
    # Single query joining model_download and model_app to fetch the package_name
    query = (
        select(model_app.c.app_id)
        .select_from(model_download.join(model_app, model_download.c.app_id == model_app.c.id))
        .where(model_download.c.hash == hash_value)
    )
    
    result = await database.fetch_one(query)
    
    if not result:
        raise HTTPException(status_code=404, detail="Package name not found for the provided hash")

    return result['app_id']

# Function to create a signed URL
def create_presigned_url(hash_value: str, package_name: str, expires_delta: timedelta) -> str:
    expire = datetime.utcnow() + expires_delta
    token_data = {
        "sub": hash_value,
        "exp": expire
    }
    if package_name:
        token_data["package_name"] = package_name
    # Create a signed token
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    # Generate a URL with the signed token
    presigned_url = f"/api/download/{hash_value}?token={token}"
    return presigned_url

async def is_rate_limited(user_email: str) -> bool:
    key = f"download_rate:{user_email}"
    redis_client = get_redis()
    current_downloads = await redis_client.get(key)

    if current_downloads and int(current_downloads) >= 10:
        return True
    else:
        await redis_client.incr(key)
        await redis_client.expire(key, 3600)  # Reset the counter every hour
        return False
    
async def log_download_activity(user_email: str, hash_value: str, request: Request, database):
    """
    Log the download activity with the user's email, hash, user-agent, and IP address.
    """
    try: 
        user_agent = request.headers.get('user-agent', 'Unknown')
        ip_address = request.headers.get('x-forwarded-for', request.client.host)

        log_data = {
            "email": user_email,
            "hash": hash_value,
            "user_agent": user_agent,
            "ip_address": ip_address,
        }
        
        query = insert(download_log).values(**log_data)
        await database.execute(query)
    
    except MySQLError as db_error:
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@router.get("/generate-download-url/{hash_value}")
async def generate_download_url(hash_value: str, request: Request, user: dict = Depends(get_current_user), database: Database = Depends(get_database)):
    try:
        # Check for rate limiting
        if await is_rate_limited(user["email"]):
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again after an hour!")
        
        # Check if the file exists in one of the specified directories
        find_file_path(hash_value)

        package_name = await get_package_name(hash_value, database)

        # Generate a pre-signed URL with a token
        presigned_url = create_presigned_url(hash_value, package_name, timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))

        await log_download_activity(user["email"], hash_value, request, database)

        return JSONResponse({"url": presigned_url})
    
    except HTTPException as http_exc:
        raise http_exc
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print("Exception in generating download url: "+ str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/{hash_value}")
async def download_file(hash_value: str, token: str):
    try:
        # Decode the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload["sub"] != hash_value:
            raise HTTPException(status_code=403, detail="Invalid token")
        
        package_name = payload["package_name"] if payload["package_name"] else ""
            
        # Verify the file's existence again (defensive check)
        file_path = find_file_path(hash_value)
    
        def file_generator():
            with open(file_path, "rb") as f:
                while chunk := f.read(1024 * 1024):
                    yield chunk

        # Serve the file from the identified path
        return StreamingResponse(file_generator(), media_type='application/octet-stream', headers={
            'Content-Disposition': f'attachment; filename="{package_name}-{hash_value}.apk"'
        })

    except HTTPException as http_exc:
        raise http_exc
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=403, detail="Invalid token")
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# BATCH_SIZE = 1000  # Adjust as needed
# NUM_WORKERS = 10  # Number of concurrent workers

# @router.delete("/admin/_index")
# async def delete_index(index_name: str):
#     """
#     Deletes an index with the specified name. If the index does not exist, raises a 404 error.
#     """
#     es_client = get_elasticsearch_async()
    
#     try:
#         # Attempt to delete the index
#         await es_client.indices.delete(index=index_name)
#         return {"message": f"Index '{index_name}' deleted successfully."}
#     except Exception as e:
#         # Handle other exceptions
#         raise HTTPException(status_code=500, detail=str(e))

# @router.post("/admin/_index")
# async def create_index(index_name: str):
#     """
#     Creates a new index with the specified name. If the index already exists, the request fails.
#     """
#     es_client = get_elasticsearch_async()

#     # Check if the index already exists
#     if await es_client.indices.exists(index=index_name):
#         raise HTTPException(status_code=400, detail=f"Index '{index_name}' already exists.")

#     # Define the index mapping with expanded fields
#     index_body = {
#         "mappings": {
#             "properties": {
#             "app_id": { "type": "keyword" },
#             "package_name": {
#                 "type": "text",
#                 "fields": {
#                 "raw": {
#                     "type": "keyword"
#                 }
#                 }
#             },
#             "developer_name": { "type": "text" },
#             "categories": { "type": "keyword" },
#             "names": {
#                 "type": "nested",
#                 "properties": {
#                 "name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
#                 "created_on": { "type": "date" }
#                 }
#             },
#             "descriptions": {
#                 "type": "nested",
#                 "properties": {
#                 "description": { "type": "text" },
#                 "created_on": { "type": "date" }
#                 }
#             },
#             "versions": {
#                 "type": "nested",
#                 "properties": {
#                 "id": { "type": "keyword" },
#                 "permissions": {
#                     "type": "text",
#                     "fields": {
#                     "raw": {
#                         "type": "keyword"
#                     }
#                     }
#                 }
#                 }
#             }
#             }
#         }
#     }

#     # Create the index
#     await es_client.indices.create(index=index_name, body=index_body)
#     print(f"Created index: {index_name}")
#     return {"index_name": index_name}

# @router.post("/admin/fill_index")
# async def fill_index(response: Response, background_tasks: BackgroundTasks, index_name: str = Query(..., description="Name of the index to fill with data")):
#     """
#     Fills the specified index with data in the background. If the index does not exist, the request fails.
#     """
#     es_client = get_elasticsearch_async()
    
#     # Check if the index exists
#     if not await es_client.indices.exists(index=index_name):
#         raise HTTPException(status_code=404, detail=f"Index '{index_name}' does not exist.")

#     # Start filling the index in the background
#     background_tasks.add_task(fill_index_task, index_name)
#     response.status_code = 202  # Accepted
#     return {"status": f"Indexing started for index '{index_name}' in the background."}

# async def fetch_batch(offset, db, index_name):
#     # Step 1: Fetch app IDs and package names in batch
#     app_query = select(model_app.c.id.label("app_id"), model_app.c.app_id.label("package_name")).limit(BATCH_SIZE).offset(offset)
#     apps = await db.fetch_all(app_query)
    
#     if not apps:
#         return None  # No more data to process
    
#     app_ids = [app["app_id"] for app in apps]
#     app_data = {app["app_id"]: {"package_name": app["package_name"], "names": [], "descriptions": []} for app in apps}

#     # Step 2: Fetch names for the batch of app IDs
#     name_query = select(model_name.c.app_id, model_name.c.name, model_name.c.created_on).where(model_name.c.app_id.in_(app_ids))
#     names = await db.fetch_all(name_query)
    
#     for row in names:
#         app_data[row["app_id"]]["names"].append({
#             "name": row["name"],
#             "created_on": row["created_on"].strftime("%Y-%m-%d") if row["created_on"] else None
#         })

#     # Step 3: Fetch descriptions for the batch of app IDs
#     description_query = select(model_description.c.app_id, model_description.c.text.label("description"), model_description.c.created_on).where(model_description.c.app_id.in_(app_ids))
#     descriptions = await db.fetch_all(description_query)
    
#     for row in descriptions:
#         app_data[row["app_id"]]["descriptions"].append({
#             "description": row["description"],
#             "created_on": row["created_on"].strftime("%Y-%m-%d") if row["created_on"] else None
#         })

#     # Prepare documents for bulk indexing
#     actions = [
#         {
#             "_index": index_name,
#             "_id": app_id,
#             "_source": {
#                 "app_id": app_id,
#                 "package_name": data["package_name"],
#                 "names": data["names"],
#                 "descriptions": data["descriptions"]
#             }
#         }
#         for app_id, data in app_data.items()
#     ]
    
#     return actions

# async def fetch_batch_all_data(offset, db, index_name, worker_id):
#     # # Step 1: Fetch a limited set of IDs based on the offset to ensure the batch is within bounds
#     # id_query = """
#     # SELECT id FROM model_app ORDER BY id LIMIT :limit OFFSET :offset
#     # """
#     # limited_app_ids = await db.fetch_all(query=id_query, values={"limit": BATCH_SIZE, "offset": offset})
#     # #print(f'Worker {worker_id}: Finished step 1')
#     # # Check if there are any IDs to process
#     # if not limited_app_ids:
#     #     print(f"No records found for offset {offset}. Ending this worker's task.")
#     #     return None  # No more data to process for this worker

#     # # Extract IDs as a tuple for use in the main query
#     # app_ids = tuple(row["id"] for row in limited_app_ids)

#     # # Step 2: Main data fetching query with a JOIN to limited_app_ids
#     # main_query = """
#     # SELECT 
#     #     ma.id AS app_id, 
#     #     ma.app_id AS package_name,
#     #     md.developer_id AS developer_name,
#     #     mn.name, mn.created_on AS name_date,
#     #     dsc.text AS description, dsc.created_on AS description_date,
#     #     mc.name AS category_name
#     # FROM 
#     #     model_app ma
#     # JOIN 
#     #     (SELECT id FROM model_app WHERE id IN :app_ids) AS limited_app_ids ON ma.id = limited_app_ids.id
#     # JOIN 
#     #     model_developer md ON ma.developer_id = md.id
#     # LEFT JOIN 
#     #     model_name mn ON mn.app_id = ma.id
#     # LEFT JOIN 
#     #     model_description dsc ON dsc.app_id = ma.id
#     # LEFT JOIN 
#     #     model_category_apps__model_app_categories mca ON mca.model_app_id = ma.id
#     # LEFT JOIN 
#     #     model_category mc ON mca.model_category_id = mc.id
#     # """
    
#     # # Execute the main query
#     # rows = await db.fetch_all(query=main_query, values={"app_ids": app_ids})
#     #print(f'Worker {worker_id}: Finished step 2')
#     combined_query = """
#     SELECT 
#         ma.id AS app_id, 
#         ma.app_id AS package_name,
#         md.developer_id AS developer_name,
#         mn.name, mn.created_on AS name_date,
#         dsc.text AS description, dsc.created_on AS description_date,
#         mc.name AS category_name
#     FROM 
#         model_app ma
#     JOIN 
#         (SELECT id FROM model_app ORDER BY id LIMIT :limit OFFSET :offset) AS limited_app_ids
#         ON ma.id = limited_app_ids.id
#     JOIN 
#         model_developer md ON ma.developer_id = md.id
#     LEFT JOIN 
#         model_name mn ON mn.app_id = ma.id
#     LEFT JOIN 
#         model_description dsc ON dsc.app_id = ma.id
#     LEFT JOIN 
#         model_category_apps__model_app_categories mca ON mca.model_app_id = ma.id
#     LEFT JOIN 
#         model_category mc ON mca.model_category_id = mc.id
#     """
#     rows = await db.fetch_all(query=combined_query, values={"limit": BATCH_SIZE, "offset": offset})

#     # Step 3: Organize data by app_id for names, descriptions, categories, etc.
#     app_data = {}
#     for row in rows:
#         app_id = row["app_id"]
#         if app_id not in app_data:
#             app_data[app_id] = {
#                 "package_name": row["package_name"],
#                 "developer_name": row["developer_name"],
#                 "names": set(),
#                 "descriptions": set(),
#                 "categories": set(),
#                 "versions": {}
#             }

#         if row["name"]:
#             app_data[app_id]["names"].add((row["name"], row["name_date"].strftime("%Y-%m-%d") if row["name_date"] else None))
#         if row["description"]:
#             app_data[app_id]["descriptions"].add((row["description"], row["description_date"].strftime("%Y-%m-%d") if row["description_date"] else None))
#         if row["category_name"]:
#             app_data[row["app_id"]]["categories"].add(row["category_name"])

#     # Step 4: Fetch versions for each app_id and associate permissions
#     version_query = """
#     SELECT 
#         md.app_id AS app_id,
#         md.id AS download_id,
#         md.version_id AS version_id
#     FROM 
#         model_download md
#     WHERE 
#         md.app_id IN :app_ids
#     """
#     version_rows = await db.fetch_all(query=version_query, values={"app_ids": tuple(app_data.keys())})
#     #print(f'Worker {worker_id}: Finished step 4')

#     # Step 5: Initialize versions and retrieve permissions from both sources
#     download_ids = [row["download_id"] for row in version_rows if row["download_id"]]
#     for row in version_rows:
#         app_id = row["app_id"]
#         version_id = row["version_id"]
#         if version_id not in app_data[app_id]["versions"]:
#             app_data[app_id]["versions"][version_id] = {"id": version_id, "permissions": set()}

#     # Fetch permissions using the first method
#     permissions_query_1 = """
#     SELECT 
#         mpr.name AS permission_name,
#         md.app_id,
#         md.version_id AS version_id
#     FROM model_permissionrequested mpr
#     JOIN model_androidmanifest mv ON mv.id = mpr.manifest_id
#     JOIN model_download md ON md.id = mv.download_id
#     WHERE md.id IN :download_ids
#     """
#     # Only run the query if download_ids is not empty
#     if download_ids:
#         permissions_rows_1 = await db.fetch_all(query=permissions_query_1, values={"download_ids": tuple(download_ids)})
#         #print(f'Worker {worker_id}: Finished step 5')
#     else:
#         #print(f'Worker {worker_id}: Skipping permissions query as download_ids is empty')
#         permissions_rows_1 = []

#     for row in permissions_rows_1:
#         app_id = row["app_id"]
#         version_id = row["version_id"]
#         permission_name = row["permission_name"]
#         if permission_name and app_id in app_data and version_id in app_data[app_id]["versions"]:
#             app_data[app_id]["versions"][version_id]["permissions"].add(permission_name)

#     # Fetch permissions using the second method
#     permissions_query_2 = """
#     SELECT 
#         mp.name AS permission_name,
#         md.app_id,
#         md.version_id AS version_id
#     FROM model_app_permissions mp
#     JOIN model_download md ON md.id = mp.download_id
#     WHERE md.id IN :download_ids
#     """
#     if download_ids:
#         permissions_rows_2 = await db.fetch_all(query=permissions_query_2, values={"download_ids": tuple(download_ids)})
#     else:
#         permissions_rows_2 = []
#     #print(f'Worker {worker_id}: Finished step 6')

#     for row in permissions_rows_2:
#         app_id = row["app_id"]
#         version_id = row["version_id"]
#         permission_name = row["permission_name"]
#         if permission_name and app_id in app_data and version_id in app_data[app_id]["versions"]:
#             app_data[app_id]["versions"][version_id]["permissions"].add(permission_name)

#     # Step 5: Convert data structures for Elasticsearch and remove duplicates
#     for data in app_data.values():
#         data["names"] = [{"name": name, "created_on": date} for name, date in data["names"]]
#         data["descriptions"] = [{"description": description, "created_on": date} for description, date in data["descriptions"]]
#         data["categories"] = list(data["categories"])
#         data["versions"] = [{"id": version_id, "permissions": list(version_data["permissions"])} for version_id, version_data in data["versions"].items()]

#     # Prepare documents for bulk indexing
#     actions = [
#         {
#             "_op_type": "index",
#             "_index": index_name,
#             "_id": app_id,
#             "_source": {
#                 "app_id": app_id,
#                 "package_name": data["package_name"],
#                 "developer_name": data["developer_name"],
#                 "names": data["names"],
#                 "descriptions": data["descriptions"],
#                 "categories": data["categories"],
#                 "versions": data["versions"]
#             }
#         }
#         for app_id, data in app_data.items()
#     ]
    
#     return actions

# async def fetch_batch_with_retry(offset, db, index_name, worker_id, retries=3):
#     for attempt in range(retries):
#         try:
#             return await fetch_batch_all_data(offset, db, index_name, worker_id)
#         except Exception as e:
#             if attempt < retries - 1:
#                 print(f"******** Worker {worker_id}: Retrying fetch for offset {offset} (attempt {attempt + 1}) due to error: {e} **********")
#                 await asyncio.sleep(2 ** attempt)  # Exponential backoff
#             else:
#                 print(f"******** Worker {worker_id}: Failed to fetch batch at offset {offset} after {retries} attempts: {e} **********")
#                 raise e

# async def retry_async_bulk(es_client, actions, worker_id, max_retries=3):
#     """
#     Retryable async_bulk helper for Elasticsearch.

#     Args:
#         es_client: The AsyncElasticsearch client.
#         actions: The batch of actions to index.
#         max_retries: Maximum number of retries.
#         delay: Delay (in seconds) between retries.

#     Returns:
#         (success, failed): A tuple of successful and failed counts.
#     """
#     attempts = 0

#     while attempts < max_retries:
#         try:
#             # Attempt the async_bulk operation
#             success, failed = await helpers.async_bulk(es_client, actions, raise_on_error=False)
#             #print(f"Bulk operation succeeded. Success: {success}, Failed: {failed}")
#             return success, failed
#         except Exception as e:
#             attempts += 1
#             if attempts < max_retries:
#                 print(f"******** Worker {worker_id}: Retrying bulk op (attempt {attempts + 1}) due to error: {e} **********")
#                 await asyncio.sleep(2 ** attempts)
#             else:
#                 print(f"******** Worker {worker_id}: Failed bulk op (attempt {attempts + 1}) due to error: {e} **********")
#                 raise  # Re-raise the exception after max retries

#     # Return 0 success and failed counts if all retries fail (optional fallback)
#     return 0, len(actions)

# async def worker(worker_id, db, es_client, index_name):
#     offset = worker_id * BATCH_SIZE
#     total_docs_indexed = 0
#     worker_start_time = time.time()

#     while True:
#         batch_start_time = time.time()
#         try:
#             actions = await fetch_batch_with_retry(offset, db, index_name, worker_id)
#             if not actions:
#                 print(f"Worker {worker_id}: No more records. Finished processing.")
#                 break

#             # Bulk insert documents into Elasticsearch
#             # success, failed = await helpers.async_bulk(es_client, actions, raise_on_error=False)
#             success, failed = await retry_async_bulk(es_client, actions, worker_id)
#             batch_time = time.time() - batch_start_time
#             print(f"Worker {worker_id}: Indexed batch at offset {offset} in {batch_time:.2f} seconds")
#             print(f"Worker {worker_id}: Success: {success}, Failed: {failed}")

#             total_docs_indexed += success
#             offset += BATCH_SIZE * NUM_WORKERS
#         except Exception as e:
#             print(f"------ Worker {worker_id}: Encountered error at offset {offset} - {str(e)} ---------")
#             # Optionally, you could add a short delay here to prevent rapid retries
#             await asyncio.sleep(1)
#             break

#     worker_total_time = time.time() - worker_start_time
#     print(f"Worker {worker_id}: Total documents indexed = {total_docs_indexed}, Total time = {worker_total_time:.2f} seconds")

# async def fill_index_task(index_name):
#     es_client = get_elasticsearch_async()
#     db = get_database()

#     # Launch 10 concurrent workers
#     tasks = [worker(worker_id, db, es_client, index_name) for worker_id in range(NUM_WORKERS)]
#     await asyncio.gather(*tasks)

#     print("Indexing complete.")