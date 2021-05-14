import datetime
from typing import Dict


class Event(object):
    def __init__(self, data: Dict = {}):
        self.data = data
        self.created_at = datetime.datetime.utcnow()

    def __str__(self) -> str:
        return str(self.data)
