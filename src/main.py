### main.py
from fastapi import FastAPI
from .api.wells_router import router as wells_router


app = FastAPI()
app.include_router(wells_router,
                   prefix="/wells_production",
                   tags=["Wells Production"])
