import argparse
import yaml
import os
from src.components.logfactory import get_logger
logger = get_logger(__name__)

# Path to save the input arguments
CONFIG_FILE = "./inputs/saved_args.yaml"

def parse_and_store_args():
    parser = argparse.ArgumentParser(description="Provide GCS folder paths")
    parser.add_argument('--src_gdrive', type=str, help='Path to source GCS folder')
    parser.add_argument('--backup', type=str, help='Path to backup folder')
    parser.add_argument('--reset', action='store_true', help='Reset saved arguments')

    args = parser.parse_args()

    # If reset or config doesn't exist, re-ask and overwrite
    if args.reset or not os.path.exists(CONFIG_FILE):
        if not args.src_gdrive or not args.backup:
            parser.error("When using --reset, both --src_gdrive and --backup must be provided.")
        
        # Save new arguments
        to_save = {
            'src_gdrive': args.src_gdrive,
            'backup': args.backup
        }

        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(to_save, f)

        logger.info("Arguments saved and reset.")
        return to_save

    # Load saved arguments if not resetting
    with open(CONFIG_FILE, 'r') as f:
        saved_args = yaml.safe_load(f)
        logger.info("Loaded saved arguments.!!")
        return saved_args
