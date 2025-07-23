import os
import platform
from dotenv import load_dotenv
from src.components.logfactory import get_logger

logger = get_logger(__name__)

load_dotenv()

def get_credential_path(base_var_name):
    system = platform.system()
    suffix_map = {
        "Darwin": "_MAC",
        "Windows": "_WIN",
        "Linux": "_LINUX"
    }
    suffix = suffix_map.get(system)
    if not suffix:
        raise ValueError(f"No credential path set for OS: {system}")

    env_var = f"{base_var_name}{suffix}"
    path = os.getenv(env_var)
    
    if not path:
        raise ValueError(f"No credential path set for environment variable: {env_var}")

    return path
