import os
from pathlib import Path
from pathlib import PurePath

def main():
    rel_path = 'src/histdatacom/__init__.py'
    here = PurePath(__file__).parent
    there = here / rel_path
    print(str(there))
    
    readme = Path("README.md")
    with readme.open("r", encoding="utf-8") as file:
        long_description = file.read()
    

    print(long_description)


if __name__ == "__main__":
    main()
