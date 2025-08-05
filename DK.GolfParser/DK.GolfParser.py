import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.entrilist import EntrylistFetcher
# from datadog_setup import setup_datadog 

if __name__ == "__main__":
    # setup_datadog()
    EntrylistFetcher().process()