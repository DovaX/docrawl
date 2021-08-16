import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='docrawl',
    version='0.1.0',
    author='DovaX',
    author_email='dovax.ai@gmail.com',
    description='Do automated crawling of pages using scrapy, selenium and other libraries',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/DovaX/docrawl',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
          'scrapy','selenium','crochet','pynput','keepvariable'
     ],
    python_requires='>=3.6',
)
    