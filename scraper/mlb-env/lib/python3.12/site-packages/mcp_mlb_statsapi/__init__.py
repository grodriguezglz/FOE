"""
This is the initialization file for the package.
"""
from .server import mcp
import sys

def main():
    mcp.run(transport='stdio')
    print('...', file=sys.stderr)

if __name__ == "__main__":
    main()