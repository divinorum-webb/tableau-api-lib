from functools import wraps

from tableau_api_lib.exceptions import InvalidParameterException


def validate_schedule_state_override(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        state_override_value = kwargs['state_override_value'] if 'state_override_value' in kwargs.keys() else None
        state_override_value = state_override_value.lower().capitalize() if state_override_value else None
        if state_override_value in ['Suspended', 'Active', None]:
            return func(*args, **kwargs)
        else:
            raise InvalidParameterException(func.__name__, state_override_value)
    return wrapper
