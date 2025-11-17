"""Setup script for gslides_automator package."""

from setuptools import setup, find_packages

# Read long description from README
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "Generate data and reports from Google Sheets and Drive"

# Dependencies - hardcoded to avoid FileNotFoundError during build
install_requires = [
    "gspread>=5.0.0",
    "google-auth>=2.0.0",
    "google-auth-httplib2>=0.1.0",
    "google-api-python-client>=2.0.0",
    "pandas>=1.5.0",
]

setup(
    name="gslides_automator",
    version="0.1.0",
    author="",  # Add your name or organization here
    author_email="",  # Add your email here
    description="Generate data and reports from Google Sheets and Drive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rvignesh89/gslide_automator",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",  # Update if using a different license
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.12",
    install_requires=install_requires,
    extras_require={
        "dev": [],
    },
)

