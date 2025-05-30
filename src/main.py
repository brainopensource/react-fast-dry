### main.py
from fastapi import FastAPI
from src.api.generic_dataset_router import router as generic_dataset_router


app = FastAPI()
app.include_router(generic_dataset_router, prefix="", tags=["Generic Dataset"])
