import re
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
    ELEMENT = 'element'
    COOKIES = 'cookies'


@dataclass
class Element:
    name: str
    type: str   # TODO: change to ElementType
    rect: Dict[str, dict]
    xpath: str
    data: Dict[str, dict]

    def dict(self):
        return asdict(self)


def classify_element_by_xpath(xpath: str) -> str:
    xpath_split = re.split('//|/', xpath)  # Split XPath into parts
    last_element_in_xpath = xpath_split[-1]  # Last element in XPath

    # Default element's type
    element_type_classified = 'element'

    # Try to find last element in XPath in predefined tags to identify element name
    for element_type, predefined_tags in PREDEFINED_TAGS.items():
        if any([x == last_element_in_xpath for x in predefined_tags]):
            element_type_classified = element_type
            break

    return element_type_classified


# Predefined tags by type
TABLE_TAGS = ['table']
BULLET_TAGS = ['ul', 'ol']
TEXT_TAGS = ['p', 'strong', 'em', 'div[normalize-space(text())]', 'span[normalize-space(text())]']
HEADLINE_TAGS = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
IMAGE_TAGS = ['img']
BUTTON_TAGS = ['button', 'a[@role="button"]', 'a[contains(@class, "button")]',
               'a[contains(@id, "button")]', 'a[@type="button"]', 'a[contains(@class, "btn")]']

# <a> tags, excluding links in menu, links as images, mailto links and links with scripts
LINK_TAGS = ["""
                a[@href
                and not(contains(@id, "Menu"))  
                and not(contains(@id, "menu"))  
                and not(contains(@class, "Menu"))  
                and not(contains(@class, "menu"))   
                and not(descendant::img) 
                and not(descendant::svg)  
                and not(contains(@href, "javascript"))  
                and not(contains(@href, "mailto"))]
                """]

# All predefined tags
PREDEFINED_TAGS = {
    ElementType.TABLE: TABLE_TAGS,
    ElementType.BULLET: BULLET_TAGS,
    ElementType.TEXT: TEXT_TAGS,
    ElementType.HEADLINE: HEADLINE_TAGS,
    ElementType.IMAGE: IMAGE_TAGS,
    ElementType.BUTTON: BUTTON_TAGS,
    ElementType.LINK: LINK_TAGS + ['a']      # + ['a'] is to identify link tags when using custom XPath
}

