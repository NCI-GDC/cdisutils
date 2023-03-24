class UserError(Exception):
    def __init__(self, message, code=400, json={}):
        self.json = json
        self.message = message
        self.code = code


class InternalError(Exception):
    def __init__(self, message=None, code=500):
        self.message = "Internal server error"
        if message:
            self.message += f': {message}'
        self.code = code
