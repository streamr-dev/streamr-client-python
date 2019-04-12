"""
return the api_key for testing
"""
from pathlib import Path
import json

__all__ = ['get_api_key']


def get_api_key():
    """
    return the api_key stored in env file
    :return:
    """
    env = Path('tests/env').open('r')

    try:
        keys = json.load(env)
        return keys['api_key']

    except json.JSONDecodeError:
        env.close()
        import traceback
        traceback.print_exc()
        return None
