from setuptools import setup, find_packages

setup(
    name="well-production-api",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "pydantic",
        "pydantic-settings",
        "duckdb",
        "polars",
        "httpx",
        "python-dotenv"
    ],
    python_requires=">=3.8",
) 