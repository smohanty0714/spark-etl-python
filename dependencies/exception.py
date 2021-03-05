"""
exception.py
~~~~~~~~

Module containing Custom Exceptions
"""


class UdfUnavailable(Exception):
    """Raised when udf specified not available"""

    def __init__(self, e):
        raise e
