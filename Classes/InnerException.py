import pprint
import traceback
import sys

class InnerException(Exception):
    def __init__(self, ex, function_name, local_obj):
        super().__init__(str(ex))
        self.original_exception = ex

        # Traceback info
        exc_type, exc_value, exc_traceback = sys.exc_info()
        file_name = traceback.extract_tb(exc_traceback)[-1].filename
        line_number = traceback.extract_tb(exc_traceback)[-1].lineno
        info = {"file_name": file_name,
                "function": function_name,
                "line_number": line_number}

        # If the original exception is InnerException, append new local_obj to its local_obj
        if isinstance(ex, InnerException):
            self.local_obj = {**local_obj, **ex.local_obj}
            self.call_stack = ex.call_stack + [info]
        else:
            self.local_obj = local_obj
            self.call_stack = [info]

        self.email_body = self.get_email_body()

    def get_email_body(self):
        email_body = "An exception has occurred in your application:\n\n"
        email_body += "Original error message: {}\n\n".format(str(self))
        email_body += "The error occurred in function '{}' at line number {} in file '{}'\n\n".format(
            self.call_stack[0]['function'],
            self.call_stack[0]['line_number'],
            self.call_stack[0]['file_name']
        )
        email_body += "Local object states at the point of exception:\n"
        for key, value in self.local_obj.items():
            email_body += "{}: {}\n".format(key, pprint.pformat(value))

        return email_body
