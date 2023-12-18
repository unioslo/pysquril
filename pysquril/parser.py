
"""SQURIL - Structured Query URI Language."""

import json
import re

from abc import ABC, abstractmethod
from typing import Optional, Union, Callable
from urllib.parse import unquote

from pysquril.exc import ParseError

class SelectElement(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    @property
    @abstractmethod
    def regex(self) -> str:
        pass


class BaseSelectElement(SelectElement):

    name = None
    regex = None

    def __init__(self, element: str) -> None:
        self.element = element
        self.bare_key = self.create_bare_key(self.element)
        self.sub_selections = self.create_sub_selections(self.element)
        self.idx = self.create_idx(self.element)

    def create_bare_key(self, element: str) -> Optional[list]:
        return element.split('[')[0] if '[' in element else None

    def create_sub_selections(self, element: str) -> list:
        return element.split('|')[1].replace(']', '').split(',') if '|' in element else []

    def create_idx(self, element: str) -> Optional[str]:
        if '[' in element and '|' in element:
            return re.sub(r'.+\[(.*)\|(.*)\]', r'\1', element)
        elif '[' in element and '|' not in element:
            return re.sub(r'.+\[(.*)\]', r'\1', element)
        else:
            return None


class Key(BaseSelectElement):
    name = 'key'
    regex = r'[^\[\]]+$'


class ArraySpecific(BaseSelectElement):
    name = 'array.specific'
    regex = r'.+\[[0-9]+\]$'


class ArraySpecificSingle(BaseSelectElement):
    name = 'array.specific.single'
    regex = r'.+\[[0-9]+\|[^,]+\]$'


class ArraySpecificMultiple(BaseSelectElement):
    name = 'array.specific.multiple'
    regex = r'.+\[[0-9]+\|.+,.+\]$'


class ArrayBroadcastSingle(BaseSelectElement):
    name = 'array.broadcast.single'
    regex = r'.+\[\*\|[^,]+\]$'


class ArrayBroadcastMultiple(BaseSelectElement):
    name = 'array.broadcast.multiple'
    regex = r'.+\[\*\|.+,.+\]$'


class SelectTerm(object):

    supported_functions = [
        'count', 'avg', 'sum', 'min', 'max', 'min_ts', 'max_ts',
    ]

    def __init__(self, original: str) -> None:
        self.func, self.original = self.strip_function(original)
        self.bare_term = self.original.split('[')[0]
        self.parsed = self.parse_elements()

    def strip_function(self, term: str) -> tuple:
        func = None
        for sf in self.supported_functions:
            if term.startswith(f"{sf}("):
                func = sf
                term = term.replace(f"{sf}(", "")[:-1]
                break
        return func, term

    def parse_elements(self) -> list:
        out = []
        parts = self.original.split('.')
        for element in parts:
            element_instance = None
            found = False
            for ElementClass in [
                Key,
                ArraySpecific,
                ArraySpecificSingle,
                ArraySpecificMultiple,
                ArrayBroadcastSingle,
                ArrayBroadcastMultiple
            ]:
                if re.match(ElementClass.regex, element):
                    if found:
                        msg = f'Could not uniquely identify {element} - already matched with {found}'
                        raise ParseError(msg)
                    element_instance = ElementClass(element)
                    found = ElementClass.name
            if not element_instance:
                raise ParseError(f'Could not parse {element}')
            out.append(element_instance)
        return out


class WhereElement(object):

    def __init__(
        self,
        groups: list,
        combinator: str,
        term: str,
        op: str,
        val: str,
    ) -> None:
        self.groups_start, self.groups_end = self.categorise_groups(groups)
        self.combinator = combinator
        self.select_term = SelectTerm(term)
        self.op = op
        self.val = val

    def categorise_groups(self, groups: list) -> tuple:
        start, end = [], []
        for bracket in groups:
            if bracket == '(':
                start.append(bracket)
            elif bracket == ')':
                end.append(bracket)
        return start, end


class WhereTerm(object):

    combinators = ['and:', 'or:']

    def __init__(self, original: str) -> None:
        self.original = original
        self.parsed = self.parse_elements_quoted()

    def parse_elements_quoted(self) -> list:
        element = self.original
        temp = ""
        groups = []
        # find groups, remove brackets
        is_quoted = False
        for token in element:
            if token == "'":
                is_quoted = not is_quoted
            if token in ['(', ')'] and not is_quoted:
                groups.append(token)
            else:
                temp += token
        element = temp # with groups removed
        # find and remove logical operators
        combinator = None
        for c in self.combinators:
            if element.startswith(c):
                combinator = c.replace(':', '')
                element = element[len(c):]
        # find term, operator, and value: {term}={op}.{val}
        term, term_found = "", False
        op, op_found = "", False
        val = ""
        negated_ops = 0
        for token in element:
            if not term_found:
                if token != "=":
                    term += token
                else:
                    term_found = True
            else:
                if not op_found:
                    if token != ".":
                        op += token
                    else:
                        if op.startswith("not"):
                            if token == ".":
                                if negated_ops == 1:
                                    op_found = True
                                    continue
                                negated_ops += 1
                                op += token
                        else:
                            op_found = True
                else:
                    if token != "'":
                        val += token
        if op == "not.is":
            op = "is.not"
        return [WhereElement(groups, combinator, term, op, val)]


class OrderElement(object):

    def __init__(self, term: str, direction: str) -> None:
        self.select_term = SelectTerm(term)
        self.direction = direction


class OrderTerm(object):

    def __init__(self, original: str) -> None:
        self.original = original
        self.parsed = self.parse_elements()

    def parse_elements(self) -> list:
        parts = self.original.split('.')
        term = '.'.join(parts[:-1])
        direction = parts[-1]
        return [OrderElement(term, direction)]


class RangeElement(object):

    def __init__(self, start: str, end: str) -> None:
        self.start = start
        self.end = end


class RangeTerm(object):

    def __init__(self, original: str) -> None:
        self.original = original
        self.parsed = self.parse_elements()

    def parse_elements(self) -> list:
        start, end = self.original.split('.')
        return [RangeElement(start, end)]


class SetElement(object):

    def __init__(self, term: str) -> None:
        self.select_term = SelectTerm(term)
        if not isinstance(self.select_term.parsed[0], Key):
            raise ParseError(f'{term} must be an instance of Key')
        if not len(self.select_term.parsed) == 1:
            # note: relaxing this would require changes to table_restore
            raise ParseError(f'SetElements can only be top level keys - {term} is nested')


class SetTerm(object):

    def __init__(self, original: str) -> None:
        self.original = original
        self.parsed = self.parse_elements()

    def parse_elements(self) -> list:
        return [SetElement(self.original)]


class Clause(object):

    term_class = None

    def __init__(self, original: str) -> None:
        self.original = original
        self.parsed = self.parse_terms()

    def split_clause(self) -> list:
        braces_open = False
        temp = ""
        parts = []
        is_quoted = False
        for token in self.original:
            if token == "'":
                is_quoted = not is_quoted
            if not is_quoted:
                if token == '[':
                    braces_open = True
                if token == ']':
                    braces_open = False
            if token == ",":
                if braces_open or is_quoted:
                    temp += token
                else:
                    parts.append(temp)
                    temp = ""
            else:
                temp += token
        if temp:
            parts.append(temp)
        return parts

    def parse_terms(self) -> list:
        out = []
        terms = self.split_clause()
        for term in terms:
            out.append(self.term_class(term))
        return out


class SelectClause(Clause):
    term_class = SelectTerm

class WhereClause(Clause):
    term_class = WhereTerm

class OrderClause(Clause):
    term_class = OrderTerm

class RangeClause(Clause):
    term_class = RangeTerm

class SetClause(Clause):
    term_class = SetTerm


class UriQuery(object):

    """
    Lex and parse a URI query into a UriQuery object:

        Query
            -> Clause(s)
                -> [Term(s)]
                    -> [Element(s)]

    """

    def __init__(
        self,
        table_name: str,
        uri_query: str,
        data: Union[list, dict] = None,
    ) -> None:
        self.table_name = table_name
        self.original = uri_query
        self.data = data
        self.select = self.parse_clause(prefix='select=', Cls=SelectClause)
        self.where = self.parse_clause(prefix='where=', Cls=WhereClause)
        self.order = self.parse_clause(prefix='order=', Cls=OrderClause)
        self.range = self.parse_clause(prefix='range=', Cls=RangeClause)
        self.set = self.parse_clause(prefix='set=', Cls=SetClause)
        self.message = self.parse_message()

    def parse_message(self) -> str:
        message = ""
        parts = self.original.split("&")
        for part in parts:
            if part.startswith("message="):
                message = unquote(part.split("=")[-1])
        return message

    def parse_clause(self, *, prefix: str, Cls: Clause) -> Clause:
        if not prefix:
            raise Exception('prefix not specified')
        if not Cls:
            raise Exception('Cls not specified')
        parts = self.original.split('&')
        for part in parts:
            if part.startswith(prefix):
                return Cls(part[len(prefix):])
