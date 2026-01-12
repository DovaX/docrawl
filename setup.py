import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='docrawl',
    version='1.4.1',
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
        'blinker==1.6.2',
        'crochet==2.1.1',
        'keepvariable==1.2.14',
        'numpy==1.26.4',
        'pandas==2.1.1',
        'psutil==6.1.1',
        'pynput==1.7.7',
        'scrapy<=2.11.2',
        'selenium-wire==5.1.0',
        'selenium==4.24.0',
        'webdriver-manager==4.0.2'
     ],
    python_requires='>=3.9',
)
    