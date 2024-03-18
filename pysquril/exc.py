
from http import HTTPStatus

class PySqurilError(Exception):
    status = HTTPStatus.BAD_REQUEST

    def __init__(self, reason: str = "") -> None:
        self.reason = reason

class ParseError(PySqurilError):
    status = HTTPStatus.BAD_REQUEST

class DataIntegrityError(PySqurilError):
    status = HTTPStatus.BAD_REQUEST

class OperationNotPermittedError(PySqurilError):
    status = HTTPStatus.BAD_REQUEST
