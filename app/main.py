from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routers import router_github, router_member, router_category, router_network, router_session, router_admin

app = FastAPI()

ALLOWED_ORIGINS = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173/",
    "http://localhost:4173",
    "http://192.168.1.162:5173/",
    "http://192.168.64.1:5173/",
]

ALLOWED_METHODS = ["*"]
ALLOWED_HEADERS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=ALLOWED_METHODS,
    allow_headers=ALLOWED_HEADERS,
)

routers = [router_github.router, router_member.router, router_category.router, router_network.router, router_session.router, router_admin.router]
for router in routers:
    app.include_router(router)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.middleware("http")
async def http_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
