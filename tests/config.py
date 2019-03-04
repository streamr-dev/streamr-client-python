
from pathlib import Path
import json

__all__=['getAPIKey']

def getAPIKey():
    env = Path('tests/env').open('r')

    try:
        keys = json.load(env)
        return keys['apiKey']
    except:
        env.close()
        return None
