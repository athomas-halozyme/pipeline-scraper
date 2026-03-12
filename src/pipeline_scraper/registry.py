
from __future__ import annotations
from typing import Dict, Type
from .parsers.base import BaseParser
from .parsers.bms import BMSParser
from .parsers.jnj import JnJParser
from .parsers.roche import RocheParser
from .parsers.takeda import TakedaParser
from .parsers.argenx import ArgenXParser

PARSER_REGISTRY: Dict[str, Type[BaseParser]] = {
    'BMS': BMSParser,
    'JnJ': JnJParser,
    'Roche': RocheParser,
    'Takeda': TakedaParser,
    'ArgenX': ArgenXParser,
}


def get_parser(company: str) -> BaseParser:
    if company not in PARSER_REGISTRY:
        raise KeyError(f"No parser registered for company '{company}'")
    return PARSER_REGISTRY[company]()
