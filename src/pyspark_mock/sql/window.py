


class Window:

    def __init__(self):
        self._partitionBy = None
        self._orderBy = None

    def partitionBy(self, cols):
        self._partitionBy = cols
        return self

    def orderBy(self, cols):
        self._orderBy = cols
        return self
