from fastapi import APIRouter
#from utils.hdfs_func import *

router = APIRouter()

@router.get("/")
async def func_hi():
    return {"hello": "world"}
