import setuptools

# Define the package name (must be unique)
PACKAGE_NAME = 'trade_validation_pipeline'
# Define the version
PACKAGE_VERSION = '1.0.0'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    # Installation requirements: Include the core Apache Beam SDK
    # with the necessary Google Cloud Platform dependencies.
    install_requires=[
        # Use a pinned version for stability, e.g., 2.53.0
        'apache-beam[gcp]==2.53.0',
        # Add any other libraries your custom code imports:
        # 'requests',
        # 'pandas',
    ],
    # Automatically find all packages in the current directory
    packages=setuptools.find_packages(),
    # Include non-Python files (optional, but good practice)
    include_package_data=True,

    # Metadata for the package
    description='Apache Beam Pipeline for real-time trade versioning and validation.',
    author='Ramesh R',
    license='Apache-2.0',
    # Ensure standard coding format and compatibility
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.13',
    ],
)