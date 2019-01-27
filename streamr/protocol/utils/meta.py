

# auto registe subclasses to Class response

class ResponseMeta(type):

    messageClassByMessageType = {}

    @classmethod
    def register(cls,clazz,typez):
        cls.messageClassByMessageType[typez] = clazz

    def __new__(cls,name,base,attrs):
        
        clazz = super().__new__(cls, name, base, attrs)
        if name not in ['Response', 'ResendResponse']:
            cls.register(clazz, attrs['TYPE'])
        return clazz


class RequestMeta(type):

    messageClassByMessageType = {}

    @classmethod
    def register(cls, clazz, typez):
        cls.messageClassByMessageType[typez] = clazz

    def __new__(cls, name, base, attrs):

        clazz = super().__new__(cls, name, base, attrs)
        if name not in ['Request']:
            cls.register(clazz, attrs['TYPE'])
        return clazz
