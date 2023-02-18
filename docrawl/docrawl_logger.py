from enum import Enum
from datetime import datetime


class ConsoleColors(Enum):
    """
    Enum for colored output in console.
    """

    CEND = '\33[0m'
    CRED = '\33[31m'
    CGREEN = '\33[32m'
    CYELLOW = '\33[33m'
    CWHITE = '\033[0m'

    def __str__(self):
        return str(self.value)


class DocrawlLogger:
    def __init__(self):
        pass

    def _map_log_level_to_color(self, log_level: str):
        mapping_dict = {
            'info': ConsoleColors.CWHITE,
            'warning': ConsoleColors.CYELLOW,
            'success': ConsoleColors.CGREEN,
            'error': ConsoleColors.CRED,
        }

        color = mapping_dict[log_level]

        return color

    def _print_message(self, message, color):
        prefix = f'{datetime.now().strftime("%H:%M:%S")} [DOCRAWL]'

        print(f'{color}{prefix} {message} {ConsoleColors.CEND}')

    def info(self, message: str):
        color = self._map_log_level_to_color('info')

        self._print_message(message, color)

    def success(self, message: str):
        color = self._map_log_level_to_color('success')

        self._print_message(message, color)

    def warning(self, message: str):
        color = self._map_log_level_to_color('warning')

        self._print_message(message, color)

    def error(self, message: str):
        color = self._map_log_level_to_color('error')

        self._print_message(message, color)


docrawl_logger = DocrawlLogger()
