from fastapi import APIRouter, HTTPException, Query, Response, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, extract, and_
from sqlalchemy.dialects import postgresql
from typing import List, Optional
from databases import Database
from datetime import datetime
import time
import paramiko
import os
import json

from ..config import get_database, get_redis
from ..models import model_description, model_app, model_developer, model_category_apps__model_app_categories, model_category, model_user
from ..models import model_name, model_download, model_version, model_androidmanifest, model_app_permissions, model_permissionrequested, model_rating
from ..schemas import ModelName, ModelDescription, DownloadDetails
from .user_routes import get_current_user

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
        await redis_client.set(cache_result_key, json.dumps(results), ex=600)  # Cache for 10 minutes
    else:
        results = json.loads(results)
        await redis_client.expire(cache_result_key, 600)
    main_query_time = time.time() - start_time
    print(f"Main query time: {main_query_time:.2f} seconds")

    cache_params.pop("page", None)
    cache_params.pop("limit", None)
    cache_count_key = f"count:{cache_params}"
    total_count = await redis_client.get(cache_count_key)
    start_time = time.time()
    if total_count is None:
        total_count = await database.fetch_val(count_stmt)
        await redis_client.set(cache_count_key, total_count, ex=600)  # Cache for 10 minutes
    else:
        total_count = int(total_count)
        await redis_client.expire(cache_count_key, 600)
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
    query = select(model_category.c.name)
    results = await database.fetch_all(query)
    
    if not results:
        raise HTTPException(status_code=404, detail="No categories found")

    return list(set([result['name'] for result in results]))

# List of probable root folders
# ROOT_FOLDERS = [
#     "/home/ubuntu/android/",
#     "/home/ubuntu/android/androzoo",
#     "/home/ubuntu/android/androzoo/2018_n_after",
#     "/home/ubuntu/android/2018",
#     "/home/ubuntu/android/old_apks"
# ]

# def find_remote_file_path(hash_value):
#     for root in ROOT_FOLDERS:
#         remote_path = os.path.join(root, hash_value[:1], hash_value[1:2], hash_value[2:3], hash_value[3:4], hash_value[4:5], hash_value[5:6], hash_value)
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         ssh.connect("10.90.78.78", username="ubuntu")
#         sftp = ssh.open_sftp()
#         try:
#             sftp.stat(remote_path)  # Check if the file exists
#             return remote_path, ssh, sftp
#         except FileNotFoundError:
#             sftp.close()
#             ssh.close()
#     return None, None, None

# def stream_file_from_remote(hash_value: str):
#     remote_path, ssh, sftp = find_remote_file_path(hash_value)
    
#     if not remote_path:
#         raise HTTPException(status_code=404, detail="File not found")
    
#     print(remote_path)
#     try:
#         remote_file = sftp.open(remote_path, 'rb')
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to open remote file: {str(e)}")
    
#     def file_generator():
#         try:
#             while True:
#                 data = remote_file.read(1024 * 1024)  # Read in 1MB chunks
#                 if not data:
#                     break
#                 yield data
#         finally:
#             remote_file.close()
#             sftp.close()
#             ssh.close()
    
#     return file_generator

# @router.get("/download/{hash_value}")
# async def download_file(hash_value: str):
#     try:
#         file_generator = stream_file_from_remote(hash_value)
#         headers = {
#             'Content-Disposition': f'attachment; filename="{hash_value}"',
#             'Content-Type': 'application/octet-stream'
#         }
#         return StreamingResponse(file_generator(), media_type='application/octet-stream', headers=headers)
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

def stream_file_from_remote(hash_value: str):
    base_dirs = [
        "/home/ubuntu/android/",
        "/home/ubuntu/android/androzoo",
        "/home/ubuntu/android/androzoo/2018_n_after",
        "/home/ubuntu/android/2018",
        "/home/ubuntu/android/old_apks"
    ]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("10.90.78.78", username="ubuntu")
    
    sftp = ssh.open_sftp()
    
    remote_file = None

    for base_dir in base_dirs:
        remote_path = os.path.join(base_dir, hash_value[:1], hash_value[1:2], hash_value[2:3], hash_value[3:4], hash_value[4:5], hash_value[5:6], hash_value)
        print(remote_path)
        try:
            remote_file = sftp.open(str(remote_path), 'rb')
            break
        except FileNotFoundError:
            continue
    
    
    if remote_file is None:
        sftp.close()
        ssh.close()
        raise HTTPException(status_code=404, detail="File not found")
    
    def file_generator():
        try:
            while True:
                data = remote_file.read(1024 * 1024)  # Read in 1MB chunks
                if not data:
                    break
                yield data
        except Exception as e:
            raise HTTPException(status_code=500, detail="File corrupted")
        finally:
            remote_file.close()
            sftp.close()
            ssh.close()
    
    return file_generator

@router.get("/download/{hash_value}")
async def download_file(hash_value: str, user: dict = Depends(get_current_user)):
    try:
        file_generator = stream_file_from_remote(hash_value)
        headers = {
            'Content-Disposition': f'attachment; filename="{hash_value}"',
            'Content-Type': 'application/octet-stream'
        }
        return StreamingResponse(file_generator(), media_type='application/octet-stream', headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))