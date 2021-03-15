import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="autouri",
    version="0.2.4",
    python_requires=">=3.6",
    scripts=["bin/autouri"],
    author="Jin wook Lee",
    author_email="leepc12@gmail.com",
    description="Automatic localization for various URIs (s3://, gs://, http://, https:// and local path)",
    long_description="https://github.com/ENCODE-DCC/autouri",
    long_description_content_type="text/markdown",
    url="https://github.com/ENCODE-DCC/autouri",
    packages=setuptools.find_packages(exclude=["docs", "test"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    install_requires=[
        "requests",
        "pyopenssl",
        "google-cloud-storage",
        "boto3",
        "awscli",
        "dateparser",
        "filelock",
        "six>=1.13.0",
    ],
)
