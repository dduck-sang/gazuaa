import uvicorn
from fastapi import FastAPI
from routers.router_test import router

def create_app():
    app = FastAPI()
    app.include_router(router)
    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host ="0.0.0.0", port=3300)
