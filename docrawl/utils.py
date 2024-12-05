import logging
from typing import Optional
from urllib.parse import urlparse


def build_abs_url(scraped_url: str, domain_url: Optional[str] = None) -> str:
    """Build an absolute url from a scraped url and a domain url. """
    def _compute_next_path_segment(segment: str, segment_list: list[str]) -> str:
        if segment == '':
            return segment_list
        elif segment == '.':
            return segment_list
        elif segment == '..':
            try:
                segment_list.pop()
            except IndexError as e:
                raise ValueError("Relative link points to a path that does not exist.") from e
            return segment_list
        else:
            segment_list.append(segment)
            return segment_list

    scraped_url = urlparse(scraped_url)
    # If the scraped url is already an absolute url, return it.
    if scraped_url.netloc != '':
        return scraped_url.geturl()

    if domain_url is None and scraped_url.netloc == '':
        raise ValueError("The domain url must be provided if the scraped url is a relative url.")

    domain_url = urlparse(domain_url)

    if scraped_url.path.startswith('/'): # Domain-relative link, e.g. /v1/api/boop
        return f"{domain_url.scheme}://{domain_url.netloc}{scraped_url.path}"
    else: # Path-relative link, e.g. v1/api/boop
        scraped_segments = scraped_url.path.split('/')
        path_segment_list = domain_url.path.split('/')[1:] # Skip the first empty string due to the leading slash
        for segment in scraped_segments:
            path_segment_list = _compute_next_path_segment(segment, path_segment_list)
        return f"{domain_url.scheme}://{domain_url.netloc}/{'/'.join(path_segment_list)}"


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\033[93m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


def get_logger(logger_name, filename: str = None):
    logger = logging.getLogger(logger_name)

    logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(CustomFormatter())

    if filename is not None:
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.handlers.RotatingFileHandler(filename, maxBytes=100000, backupCount=3)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    logger.addHandler(stream_handler)

    return logger