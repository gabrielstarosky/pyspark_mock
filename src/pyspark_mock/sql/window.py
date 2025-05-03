


class Window:

    @staticmethod
    def partitionBy(cols):
        return WindowSpec().partitionBy(cols)

    @staticmethod
    def orderBy(self, cols):
        return WindowSpec().orderBy(cols)


class WindowSpec:

    def __init__(self):
        self._partitionBy = None
        self._orderBy = None

    def partitionBy(self, cols):
        self._partitionBy = cols
        return self

    def orderBy(self, cols):
        self._orderBy = cols
        return self