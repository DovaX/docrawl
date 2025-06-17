import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, Union, Type

import lxml
from lxml.html import HtmlElement


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
    INPUT = 'input'


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
INPUT_TAGS = 'input', 'textarea'


@dataclass
class ElementData:
    tagName: str
    textContent: str
    attributes: dict[str, Any]


@dataclass
class Element:
    name: str
    type: str   # TODO: change to ElementType
    rect: Dict[str, dict]
    xpath: str
    data: ElementData

    def dict(self):
        return asdict(self)


class AbstractElement(ABC):
    ELEMENT_TYPE: ElementType = ElementType.TEXT
    PREDEFINED_TAGS = []

    def __init__(self, html_content: str, xpath: str):
        self.html_content = html_content
        self.xpath = xpath

        self.tree: HtmlElement = lxml.html.fromstring(self.html_content)
        self.attributes = self._extract_attributes()
        self.text = self._extract_text()

        self._element_data = self.extract_data()

    @property
    def element_data(self) -> dict:
        return self._element_data

    def _extract_attributes(self) -> dict:
        """
        Extract attributes using lxml.
        """
        return {k: v for k, v in self.tree.attrib.items()}

    def _extract_text(self) -> str:
        """
        Extract text content of the element using lxml.
        """
        return self.tree.text_content().strip()

    def extract_data(self) -> dict:
        """
        Override in subclasses for element-specific data extraction.
        """
        raise NotImplementedError

    def is_sized(self) -> bool:
        """
        Determine if the element has size (can be overridden if needed).
        """
        return True

    @abstractmethod
    def is_empty(self) -> bool:
        pass


class LinkElement(AbstractElement):
    ELEMENT_TYPE = "link"
    PREDEFINED_TAGS = ["a"]

    def extract_data(self):
        """
        Extract link-specific data such as text and href attribute.
        """
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return not self.attributes.get("href")


class ButtonElement(AbstractElement):
    ELEMENT_TYPE = "button"
    PREDEFINED_TAGS = ["button", "input[@type='submit']"]

    def extract_data(self):
        """
        Extract button-specific data such as text and attributes.
        """

        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return not self.text.strip()


class ImageElement(AbstractElement):
    ELEMENT_TYPE = "image"
    PREDEFINED_TAGS = ["img"]

    def extract_data(self):
        """
        Extract image-specific data such as src and alt attributes.
        """

        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return not self.attributes.get("src")


class BulletListElement(AbstractElement):
    ELEMENT_TYPE = "bullet_list"
    PREDEFINED_TAGS = ["ul", "ol"]

    def extract_data(self):
        """
        Extract bullet list items.
        """
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return not self.tree.xpath(".//li")


class TableElement(AbstractElement):
    ELEMENT_TYPE = "table"
    PREDEFINED_TAGS = ["table"]

    def extract_data(self):
        """
        Extract table rows and columns.
        """
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return not self.tree.xpath(".//tr")


class TextElement(AbstractElement):
    ELEMENT_TYPE = ElementType.TEXT
    PREDEFINED_TAGS = TEXT_TAGS

    def extract_data(self):
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return False


class HeadlineElement(AbstractElement):
    ELEMENT_TYPE = ElementType.HEADLINE
    PREDEFINED_TAGS = HEADLINE_TAGS

    def extract_data(self):
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return False


class ContextElement(AbstractElement):
    ELEMENT_TYPE = ElementType.CONTEXT

    def extract_data(self):
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return False


class InputElement(AbstractElement):
    ELEMENT_TYPE = ElementType.INPUT
    PREDEFINED_TAGS = INPUT_TAGS

    def extract_data(self):
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return False


class CookiesElement(AbstractElement):
    ELEMENT_TYPE = ElementType.COOKIES

    def extract_data(self):
        return ElementData(self.tree.tag, self.text, self.attributes)

    def is_empty(self) -> bool:
        return False


all_types = [LinkElement, InputElement, ImageElement, TableElement, TextElement, HeadlineElement, ButtonElement, BulletListElement, CookiesElement, ContextElement]


all_elements_type = Union[LinkElement, InputElement, ImageElement, TableElement, TextElement, HeadlineElement, ButtonElement, BulletListElement, CookiesElement, ContextElement]


def classify_element_by_xpath(xpath: str) -> Type[all_elements_type]:
    xpath_split = re.split('//|/', xpath.removesuffix('/text()').rstrip('/'))  # Split XPath into parts
    last_element_in_xpath = xpath_split[-1]  # Last element in XPath

    # Default element's type
    element_type_classified = TextElement

    # Try to find last element in XPath in predefined tags to identify element name
    for element_type in all_types:
        if any([x == last_element_in_xpath for x in element_type.PREDEFINED_TAGS]):
            element_type_classified = element_type
            break

    return element_type_classified




