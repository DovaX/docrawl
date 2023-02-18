from enum import Enum
from datetime import datetime


class ConsoleColors(Enum):
    """
    Enum for colored output in console.
    """

    CEND = '\033[0m'
    CRED = '\033[31m'
    CGREEN = '\033[92m'
    CYELLOW = '\033[93m'
    CWHITE = '\033[0m'

    def __str__(self):
        return str(self.value)


class DocrawlLogger:
    def __init__(self):
        pass

    def _print_message(self, message, color):
        prefix = f'{datetime.now().strftime("%H:%M:%S")} [DOCRAWL]'

        print(f'{color}{prefix} {message} {ConsoleColors.CEND}')

    def info(self, message: str):
        color = ConsoleColors.CWHITE

        self._print_message(message, color)

    def success(self, message: str):
        color = ConsoleColors.CGREEN

        self._print_message(message, color)

    def warning(self, message: str):
        color = ConsoleColors.CYELLOW

        self._print_message(message, color)

    def error(self, message: str):
        color = ConsoleColors.CRED

        self._print_message(message, color)


docrawl_logger = DocrawlLogger()
