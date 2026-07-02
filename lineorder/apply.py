"""CLI alias: python -m lineorder.apply == lineorder.writeback.main."""

from .writeback import main

if __name__ == "__main__":
    main()
