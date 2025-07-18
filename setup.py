from setuptools import setup, find_packages

setup(
    name="web_scraping_bemol",
    version="0.1.0",
    packages=find_packages(),
    author="Caio Miguel",
    author_email="caiomiguel@bemol.com.br",
    description="Pipeline de web scraping e análise de concorrência.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/CaioMigueldeSaRodrigues/web-scraping",
    python_requires='>=3.9',
) 