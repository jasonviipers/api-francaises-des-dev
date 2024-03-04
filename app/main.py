from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import *

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173/",
    "http://localhost:4173",
    "http://192.168.1.162:5173/",
    "http://192.168.64.1:5173/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router_github.router)
app.include_router(router_member.router)
app.include_router(router_category.router)
app.include_router(router_network.router)
app.include_router(router_session.router)
app.include_router(router_admin.router)
