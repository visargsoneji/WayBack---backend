# src/routes/user.py
from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy import insert, select
from databases import Database
from datetime import datetime, timedelta
import jwt
import bcrypt

from pymysql.err import IntegrityError, MySQLError

from ..schemas import UserCreate, UserLogin
from ..models import model_user
from ..config import get_database
from ..env import SECRET_KEY, ALGORITHM


router = APIRouter()

@router.post("/register")
async def register_user(user: UserCreate, database: Database = Depends(get_database)):
    try:

        hashed_password = bcrypt.hashpw(user.password.encode('utf-8'), bcrypt.gensalt())
        
        query = insert(model_user).values(
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            password=hashed_password.decode('utf-8'),
            allow_downloads=False  # Set to False by default
        )
        
        await database.execute(query)
        return {"message": "User registered successfully"}
    
    except IntegrityError:
        raise HTTPException(status_code=400, detail=f"Email already registered: {user.email}")
    
    except MySQLError as db_error:
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


def verify_password(plain_password, hashed_password):
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


@router.post("/login")
async def login(user: UserLogin, database: Database = Depends(get_database)):
    try:
        query = select(model_user).where(model_user.c.email == user.email)
        result = await database.fetch_one(query)
        
        if not result:
            raise HTTPException(status_code=400, detail="Incorrect email")
        
        if not verify_password(user.password, result['password']):
            raise HTTPException(status_code=400, detail="Incorrect password")
        
        access_token = create_access_token(data={"sub": result['email']}, expires_delta=timedelta(days=3))
        
        return {"access_token": access_token, "token_type": "bearer"}
    
    except HTTPException as http_exc:
        raise http_exc
    except MySQLError as db_error:
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=1)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(request: Request, database: Database = Depends(get_database)):
    try:
        token = request.headers.get("Authorization")
        if token is None:
            raise HTTPException(status_code=403, detail="Not authenticated")
        
        token = token.split(" ")[1]
    
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=403)
    
        query = select(model_user).where(model_user.c.email == email)
        user = await database.fetch_one(query)
        
        if user is None or not user['allow_downloads']:
            raise HTTPException(status_code=403, detail="You don't have enough permissions. Check FAQ!")
        return user

    except HTTPException as http_exc:
        raise http_exc
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=403, detail="Error decoding user token")
    except MySQLError as db_error:
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
