import setuptools

setuptools.setup(
    name="pgcapture",
    version="0.1",
    author="rueian",
    author_github="http://github.com/rueian",
    description="Python client library for pgcapture, a scalable Netflix DBLog implementation for PostgreSQL",
    packages=["pgcapture", "pb"]
)