from setuptools import setup, find_packages

setup(
    name="web_scraping_bemol",
    version="0.1.0",
    packages=find_packages(),
    author="Caio Miguel",
    author_email="caiomiguel@bemol.com.br",
    description="Pipeline de web scraping e análise de concorrência.",
    install_requires=[
        "pyspark==3.4.1",
        "pandas==2.0.3",
        "requests==2.31.0",
        "beautifulsoup4==4.12.2",
        "sentence-transformers==2.2.2",
        "torch==2.1.0",
        "scikit-learn==1.3.1",
        "sendgrid==6.10.0",
        "openpyxl==3.1.2",
    ],
    python_requires='>=3.9',
) 