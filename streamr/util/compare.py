"""
provide a __eq__ func for comparing two message payloads
"""


class EqualFunc:
    """
    eq function for comparing two message payloads
    """
    def __eq__(self, another):
        if isinstance(another, type(self)):
            for key in self.__dict__:
                if self.__dict__[key] != another.__dict__[key]:
                    return False
            else:
                return True
        else:
            return False
