import re

def camel_to_snake(name: str) -> str:
    """
    Convert a CamelCase or PascalCase string to snake_case.

    Parameters
    ----------
    name : str
        The string in CamelCase or PascalCase format.

    Returns
    -------
    str
        The converted string in snake_case format.
    """
    if not isinstance(name, str):
        raise TypeError("Input must be a string.")

    # Add underscore before capital letters, then lowercase everything
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
