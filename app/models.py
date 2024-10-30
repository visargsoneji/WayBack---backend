from .config import metadata
from sqlalchemy import Table, Column, Integer, String, DateTime, Boolean, Float, func, Text, TIMESTAMP

model_name = Table(
    #"visarg_model_name",
    "model_name",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255), nullable=False, index=True),
    Column("created_on", DateTime, nullable=True),
    Column("app_id", Integer, nullable=False)
)


model_description =  Table(
    "model_description",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("text", String, nullable=False),
    Column("created_on", DateTime, nullable=True),
    Column("app_id", Integer, nullable=False),
)

model_app = Table(
    "model_app",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("app_id", String),
    Column("developer_id", Integer, nullable=False),
)

model_developer = Table(
    "model_developer",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("developer_id", String, nullable=False),
)

model_category_apps__model_app_categories = Table(
    "model_category_apps__model_app_categories",
    metadata,
    Column("model_app_id", Integer, nullable=False),
    Column("model_category_id", Integer, nullable=False),
)

model_category = Table(
    "model_category",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("marketplace_id", Integer, nullable=True),
    Column("created_on", DateTime, nullable=True),
)

model_download = Table(
    "model_download",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("hash", String),
    Column("app_id", Integer),
    Column("size", Integer),
    Column("created_on", DateTime),
    Column("version_id", Integer),
    Column("account_id", Integer),
    Column("device_id", Integer),
    Column("has_been_analyzed_for_load_url", Boolean),
    Column("has_been_crawled", Boolean),
    Column("is_known_malware", Boolean),
    Column("revisited_on", DateTime),
    Column("modified_on", DateTime)
)

model_version =  Table(
    "model_version",
    metadata,
     Column("id",  Integer, primary_key=True),
     Column("version",  String),
     Column("created_on",  DateTime),
     Column("app_id",  Integer)
)

model_app_permissions = Table(
    "model_app_permissions",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("download_id", Integer),
    Column("manifest_id", Integer),
    Column("name", String),
    Column("permission_type", String)
)

model_androidmanifest = Table(
    "model_androidmanifest",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("download_id", Integer),
    Column("package", String),
    Column("version_code", String),
    Column("version_name", String),
    Column("application_class_name", String),
    Column("process_name", String),
    Column("required_account_type", String),
    Column("restricted_account_type", String),
    Column("is_parsing_complete", Boolean)
)

model_sdkversion = Table(
    "model_sdkversion",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("manifest_id", Integer),
    Column("min_sdk_number", Integer),
    Column("target_sdk_number", Integer),
    Column("max_sdk_number", Integer)
)

model_permissionrequested = Table(
    "model_permissionrequested",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("manifest_id", Integer),
    Column("name", String),
    Column("max_sdk_number", Integer)
)

model_price = Table(
    "model_price",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("price", Float),
    Column("currency", String),
    Column("is_free", Boolean),
    Column("created_on", DateTime),
    Column("app_id", Integer)
)

model_rating = Table(
    "model_rating",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("rating", Float),
    Column("number_of_ratings", Integer),
    Column("one_star_ratings", Integer),
    Column("two_star_ratings", Integer),
    Column("three_star_ratings", Integer),
    Column("four_star_ratings", Integer),
    Column("five_star_ratings", Integer),
    Column("created_on", DateTime),
    Column("app_id", Integer)
)

model_user = Table(
    "users",
    metadata,
    Column("email", String, primary_key=True),
    Column("first_name", String),
    Column("last_name", String),
    Column("password", String),
    Column("allow_downloads", Boolean)
)

# Model for logging download activity
download_log = Table(
    "download_logs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String, nullable=False),
    Column("hash", String, nullable=False),
    Column("user_agent", Text, nullable=False),
    Column("ip_address", String, nullable=False),
    Column("timestamp", TIMESTAMP, default=func.now(), nullable=False),
)