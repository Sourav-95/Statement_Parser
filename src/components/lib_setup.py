from src.components.logfactory import get_logger
from typing import List, Dict
import os
import nltk
from constants import DataParserConstants

logger = get_logger(__name__)

class LibResourceManager:

    @classmethod
    def download_nlp_resource(cls, packages:List):
        """
        This function download all the packages:List if it is unable to find in the available resources
        Args:
            packages: List (Example: [words, stopwords....])

        This function is called into `data_transformer.py` and has no return.
        """
        # Ensure all required NLTK resources are available
        for resource in packages:
            try:
                nltk.data.find(f'corpora/{resource}')
            except LookupError:
                nltk.download(resource)
        
    @classmethod
    def get_custom_stopwords(cls, add_stop_words, remove_words):
        """
        This function download all the stopwords. Updates the stopword list with new words and remove few words
        Args:
            add_stop_words : [List of words which need to be added to the existing stopwords List]
            remove_words  :  [List of words which need to be removed]
        This function is called into `data_transformer.py` and has no return.
        """
        try:
            stop_words = set(nltk.corpus.stopwords.words('english'))
            stop_words.update(add_stop_words)

            for word in remove_words:
                stop_words.discard(word)
            return stop_words
        except Exception as e:
            logger.error(f'Error occurred while setting up `StopWords`: {str(e)}')
            return set(remove_words)  # Fallback: use only custom words

        
    @classmethod
    def get_custom_banking_keywords(cls, keyword_dir) -> List[str]:
        all_keywords = []
        if keyword_dir:
            try:
                with open(keyword_dir, 'r') as file:
                    for line in file:
                        if ':' in line:
                            _, keywords = line.strip().split(':', 1)
                            keyword_items = [k.strip().lower() for k in keywords.split(',')]
                            all_keywords.extend(keyword_items)
                all_keywords = list(set(all_keywords))
                return all_keywords
            except Exception as e:
                logger.error(f'Error while getting Banking keyword as : {e}')

        return all_keywords

    @classmethod
    def get_category_to_spectrum_mapper(cls, keyword_dir):
        category_map = {}
        if keyword_dir:
            try:
                with open(keyword_dir, 'r') as file:
                    for line in file:
                        if ':' in line:
                            category, vendors = line.strip().split(':', 1)
                            vendor_list = [v.strip().lower() for v in vendors.split(',')]
                            category_map[category.strip()] = vendor_list

                # Step 2: Reverse the mapping (vendor -> category)
                reverse_map = {vendor: cat for cat, vendors in category_map.items() for vendor in vendors}
                return reverse_map
            except Exception as e:
                logger.error(f'Error while getting Category to Spectrum Mapper: {e}')
        return category_map


