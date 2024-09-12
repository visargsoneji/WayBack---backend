# main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from .config import connect, disconnect, init_redis, close_redis
from .middlewares import DBConnectionMiddleware
from .routes import app_routes, user_routes

@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    await init_redis()
    yield
    await disconnect()
    await close_redis()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["x-total-count"]
)

app.add_middleware(DBConnectionMiddleware) # Maintains db connection: With long active time, OperationalError was thrown with new request

app.include_router(app_routes.router, prefix="/api")
app.include_router(user_routes.router, prefix="/api")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
