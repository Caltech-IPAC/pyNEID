from setuptools import setup, find_packages

extensions = []

reqs = []
for line in open('requirements.txt', 'r').readlines():
    reqs.append(line)

with open ("README.md", "r") as fh:
    long_description = fh.read()
    
setup(
    name="pyneid",
    version="0.1.0",
    author="Mihseh Kong",
    description="NEID archive access client", 
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url="https://github.com/Caltech-IPAC/pyNEID",
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Astronomy'],
    packages=find_packages(),
    data_files=[],
    install_requires=reqs,
    python_requires='>= 3.6',
    include_package_data=False
)
