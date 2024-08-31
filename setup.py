import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='docrawl',
    version='1.3.10',
    author='DovaX',
    author_email='dovax.ai@gmail.com',
    description='Do automated crawling of pages using scrapy',
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
          'scrapy','selenium>=4.0','crochet','pynput','keepvariable','selenium-wire','webdriver-manager','psutil','pandas'
     ],
    python_requires='>=3.6',
)
    