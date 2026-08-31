import platform
from config import appname, appversion
from .const import plugin_name, plugin_version

def generate_user_agent():
    """
    Generates a standardized User-Agent based on the host operating system
    and embeds custom application versions.
    """

    # Determine the OS platform token
    os_name = platform.system()
    machine = platform.machine()

    if os_name == "Windows":
        win_release = platform.release()
        arch = "Win64; x64" if "64" in machine else "Win32; x86"
        os_token = f"Windows NT {win_release}; {arch}"

    elif os_name == "Darwin":
        # macOS formatting
        mac_ver = platform.mac_ver()[0].replace('.', '_')
        # Map architecture names to safari conventions
        arch = "Intel Mac OS X" if "x86" in machine else "Macintosh; Intel Mac OS X"
        os_token = f"{arch} {mac_ver}"

    elif os_name == "Linux":
        os_token = f"X11; Linux {machine}"

    else:
        os_token = f"Unknown OS; {machine}"

    python_version = f"Python/{platform.python_version()}"

    # Construct a standard-compliant User-Agent
    user_agent = (
        f"Mozilla/5.0 ({os_token}) "
        f"{appname}/{appversion()} "
        f"({python_version}) "
        f"{plugin_name}/{plugin_version}"
    )

    return user_agent