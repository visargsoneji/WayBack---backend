from fastapi import APIRouter, HTTPException, Query, Response, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select, func, extract, and_
from sqlalchemy.dialects import postgresql
from typing import List, Optional
from databases import Database
from datetime import datetime, timedelta
import time
import os
import json
import jwt

from ..config import get_database, get_redis
from ..models import model_description, model_app, model_developer, model_category_apps__model_app_categories, model_category, model_user
from ..models import model_name, model_download, model_version, model_androidmanifest, model_app_permissions, model_permissionrequested, model_rating
from ..schemas import ModelName, ModelDescription, DownloadDetails
from .user_routes import get_current_user
from ..env import SECRET_KEY, ALGORITHM

ACCESS_TOKEN_EXPIRE_MINUTES = 10  # For example, URL valid for 60 minutes

router = APIRouter()

async def get_query_params(
    query: Optional[str] = None,
    package_name: Optional[str] = None,
    developer_name: Optional[str] = None,
    categories: Optional[str] = None,
    downloadable: Optional[bool] = True,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    # if not any([query, package_name, developer_name, categories]):
    #     raise HTTPException(status_code=400, detail="At least one of 'query', 'package_name', 'developer_name', 'categories' must be provided.")
    return {
        "query": query,
        "package_name": package_name,
        "developer_name": developer_name,
        "categories": categories,
        "downloadable": downloadable,
        "page": page,
        "limit": limit
    }

@router.get("/search/", response_model=List[ModelName])
async def search_apps(
    response: Response,
    params: dict = Depends(get_query_params),
    database: Database = Depends(get_database)
):
    offset = (params["page"] - 1) * params["limit"]
    name_query = f"%{params['query']}%" if params["query"] else None
    package_query = f"%{params['package_name']}%" if params["package_name"] else None
    developer_query = f"%{params['developer_name']}%" if params["developer_name"] else None
    category_query = params['categories'].split(",") if params["categories"] else None
    print('Searching initiated ...')

    # Aliases for the tables
    ma = model_app.alias("ma")
    mdev = model_developer.alias("mdev")
    mca = model_category_apps__model_app_categories.alias("mca")
    mc = model_category.alias("mc")
    mn = model_name.alias("mn")
    dl = model_download.alias("dl")
    
    base_query = (
        select(
            mn.c.id,
            mn.c.name,
            mn.c.created_on,
            mn.c.app_id,
            ma.c.app_id.label("package_name"),
            func.row_number().over(partition_by=mn.c.app_id, order_by=mn.c.created_on.desc()).label("row_num")
        ).select_from(mn.join(ma, mn.c.app_id == ma.c.id))
    )

    # Apply filters based on provided parameters
    filters = []
    if name_query:
        filters.append(mn.c.name.like(name_query))
    if package_query:
        filters.append(ma.c.app_id.like(package_query))
    if developer_query:
        filters.append(mdev.c.developer_id.like(developer_query))
        base_query = base_query.join(mdev, ma.c.developer_id == mdev.c.id)
    if category_query:
        subquery = (
            select(mca.c.model_app_id)
            .select_from(mca.join(mc, mca.c.model_category_id == mc.c.id))
            .where(mc.c.name.in_(category_query))
            .group_by(mca.c.model_app_id)
            .having(func.count(mca.c.model_app_id) >= len(category_query))
        ).alias("subquery")
        
        base_query = base_query.join(subquery, ma.c.id == subquery.c.model_app_id)
        

    if params["downloadable"]:
        base_query = base_query.join(dl, ma.c.id == dl.c.app_id)

    if filters:
        base_query = base_query.where(and_(*filters))

    
    # Apply row number filter to get the latest entry for each app_id
    filtered_query = base_query.alias("subquery")
    row_num_filtered_query = select(
        filtered_query.c.id,
        filtered_query.c.name,
        filtered_query.c.created_on,
        filtered_query.c.app_id,
        filtered_query.c.package_name
    ).where(filtered_query.c.row_num == 1)

    # Count subquery
    count_stmt = select(func.count().label("total")).select_from(row_num_filtered_query.alias("count_subquery"))

    # Apply offset and limit
    query_stmt = row_num_filtered_query.offset(offset).limit(params["limit"])

    start_time = time.time()
    # Check if the total count for this query is cached
    cache_params = params.copy()
    cache_result_key = f"result:{cache_params}"
    redis_client = get_redis()
    results = await redis_client.get(cache_result_key)
    
    start_time = time.time()
    if results is None:
        db_results = await database.fetch_all(query_stmt)
        results = [serialize_result(result) for result in db_results]
        await redis_client.set(cache_result_key, json.dumps(results), ex=36000)  
    else:
        results = json.loads(results)
        await redis_client.expire(cache_result_key, 36000)
    main_query_time = time.time() - start_time
    print(f"Main query time: {main_query_time:.2f} seconds")

    cache_params.pop("page", None)
    cache_params.pop("limit", None)
    cache_count_key = f"count:{cache_params}"
    total_count = await redis_client.get(cache_count_key)
    start_time = time.time()
    if total_count is None:
        total_count = await database.fetch_val(count_stmt)
        await redis_client.set(cache_count_key, total_count, ex=36000)  
    else:
        total_count = int(total_count)
        await redis_client.expire(cache_count_key, 36000)
    count_query_time = time.time() - start_time
    print(f"Count query time: {count_query_time:.2f} seconds")

    # Print the query in raw SQL format
    print(query_stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    if not results:
        raise HTTPException(status_code=404, detail="No matching records found")

    response.headers['x-total-count'] = str(total_count)
    return results

def serialize_result(result):
    # Convert Record to dictionary
    result_dict = dict(result)
    # Convert datetime objects to string format
    for key, value in result_dict.items():
        if isinstance(value, datetime):
            result_dict[key] = value.isoformat()  # Convert datetime to ISO format string
    return result_dict

@router.get("/details/{app_id}", response_model=ModelDescription)
async def fetchDetails(app_id: int, database: Database = Depends(get_database)):
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
    
    result = dict(result)
    result["categories"] = result["categories"].split(",") if result["categories"] else []
    
    return result

@router.get("/version-details/{app_id}", response_model=List[DownloadDetails])
async def get_version_details(app_id: int, database: Database = Depends(get_database)):
    # Aliases for the tables
    md = model_download.alias("md")
    mv = model_version.alias("mv")
    permissions_1 = model_app_permissions.alias("permissions_1")
    permissions_2 = model_permissionrequested.alias("permissions_2")
    ma = model_androidmanifest.alias("ma")
    mr = model_rating.alias("mr")
    
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
            mr.c.five_star_ratings
        )
        .select_from(
            md
            .join(mv, md.c.version_id == mv.c.id)
            .outerjoin(permissions_1, md.c.id == permissions_1.c.download_id)
            .outerjoin(ma, md.c.id == ma.c.download_id)
            .outerjoin(permissions_2, ma.c.id == permissions_2.c.manifest_id)
            .outerjoin(
                mr,
                (md.c.app_id == mr.c.app_id) &
                (extract('year', md.c.created_on) == extract('year', mr.c.created_on))
            )
        )
        .where(md.c.app_id == app_id)
        .order_by(md.c.created_on.desc()) 
    )
    
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
        
        if download_id not in download_details:
            # print(download_id, one_star_ratings, two_star_ratings, three_star_ratings, four_star_ratings, five_star_ratings)
            download_details[download_id] = {
                "hash": hash_value,
                "size": size,
                "created_on": created_on,
                "version": version,
                "permissions": set(),
        "total_ratings": number_of_ratings if number_of_ratings != None else 0,
                "rating": round(sum((i + 1) * r for i, r in enumerate([one_star_ratings, two_star_ratings, three_star_ratings, four_star_ratings, five_star_ratings])) / number_of_ratings, 2) if number_of_ratings != None and number_of_ratings != 0 else 0
            }
        
        if permission_1:
            download_details[download_id]["permissions"].add(permission_1)
        if permission_2:
            download_details[download_id]["permissions"].add(permission_2)

    return [
        DownloadDetails(
            hash=details["hash"],
            size=details["size"],
            version=details["version"],
            created_on=details["created_on"],
            permissions=list(details["permissions"]),
            total_ratings=details["total_ratings"],
            rating=details["rating"]
        )
        for details in download_details.values()
    ]

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

    categories = list(set([result['name'] for result in results]))

    await redis_client.set(cache_key, json.dumps(categories), ex=36000)  # Cache expires in 600 seconds

    return categories

# Base directories to search for the file
base_dirs = [
    "/Volumes/apks",
    "/Volumes/apks/2018",
    "/Volumes/apks/old_apks"
]

def find_file_path(hash_value: str) -> str:
    """
    Search for the file in the specified directories and return the path if found.
    """
    missing_dirs = [base_dir for base_dir in base_dirs if not os.path.exists(base_dir)]
    if missing_dirs:
        # If any directory is missing, raise an error
        raise HTTPException(status_code=500, detail=f"File server not mounted.")
    
    for base_dir in base_dirs:
        file_path = os.path.join(base_dir, hash_value[:1], hash_value[1:2], hash_value[2:3], hash_value[3:4], hash_value[4:5], hash_value[5:6], hash_value)
        if os.path.exists(file_path):
            return file_path
    raise FileNotFoundError("File not found in the specified directories.")

# Function to create a signed URL
def create_presigned_url(hash_value: str, expires_delta: timedelta) -> str:
    expire = datetime.utcnow() + expires_delta
    token_data = {
        "sub": hash_value,
        "exp": expire
    }
    # Create a signed token
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    # Generate a URL with the signed token
    presigned_url = f"/api/download/{hash_value}?token={token}"
    return presigned_url

@router.get("/generate-download-url/{hash_value}")
async def generate_download_url(hash_value: str, user: dict = Depends(get_current_user)):
    try:
        # Check if the file exists in one of the specified directories
        file_path = find_file_path(hash_value)

        # Generate a pre-signed URL with a token
        presigned_url = create_presigned_url(hash_value, timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))

        return JSONResponse({"url": presigned_url})

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/{hash_value}")
async def download_file(hash_value: str, token: str):
    try:
        # Decode the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload["sub"] != hash_value:
            raise HTTPException(status_code=403, detail="Invalid token")

        # Verify the file's existence again (defensive check)
        file_path = find_file_path(hash_value)
    
        def file_generator():
            with open(file_path, "rb") as f:
                while chunk := f.read(1024 * 1024):
                    yield chunk

        # Serve the file from the identified path
        return StreamingResponse(file_generator(), media_type='application/octet-stream', headers={
            'Content-Disposition': f'attachment; filename="{hash_value}.apk"'
        })

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=403, detail="Invalid token")
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))