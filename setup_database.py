import sys
import os
from postgre_utils import setup_database

def main():
    try:
        setup_database()
        print("\nDatabase setup completed successfully!")
    except Exception as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
