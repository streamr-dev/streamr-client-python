"""
Metaclass of Response and Request
"""


class ResponseMeta(type):
    """
    Meta class of Response
    """

    response_class_by_response_type = {}

    @classmethod
    def register(mcs, clazz, typez):
        """
        register son class to response_class_by_response_type
        :param clazz: son class
        :param typez: son class type
        :return: None
        """
        mcs.response_class_by_response_type[typez] = clazz

    def __new__(mcs, name, base, attrs):

        clazz = super().__new__(mcs, name, base, attrs)
        if name not in ['Response', 'ResendResponse']:
            mcs.register(clazz, attrs['TYPE'])
        return clazz


class RequestMeta(type):
    """
    Meta class of Request
    """

    response_class_by_response_type = {}

    @classmethod
    def register(mcs, clazz, typez):
        """
        register son class to response_class_by_response_type
        :param clazz: son class
        :param typez: son class type
        :return: None
        """
        mcs.response_class_by_response_type[typez] = clazz

    def __new__(mcs, name, base, attrs):

        clazz = super().__new__(mcs, name, base, attrs)
        if name not in ['Request']:
            mcs.register(clazz, attrs['TYPE'])
        return clazz
