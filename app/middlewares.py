# middlewares.py
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from .config import database, connect

class DBConnectionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            # Perform a simple query to test the connection
            await database.execute(text("SELECT 1"))
        except (OperationalError, Exception) as e:
            # If the connection is lost, try to reconnect
            try:
                await connect()
            except Exception as e:
                return Response("Could not reconnect to the database", status_code=500)

        response = await call_next(request)
        return response
