from enum import Enum
from dataclasses import dataclass, asdict
from typing import Dict


class ElementType(str, Enum):
    def __str__(self):
        return str(self.value)

    BULLET = 'bullet'
    LINK = 'link'
    TEXT = 'text'
    HEADLINE = 'headline'
    IMAGE = 'image'
    BUTTON = 'button'
    TABLE = 'table'
    CONTEXT = 'context'


@dataclass
class Element:
    name: str
    type: str   # TODO: change to ElementType
    rect: Dict[str]
    xpath: str
    data: Dict[str]

    def dict(self):
        return asdict(self)
