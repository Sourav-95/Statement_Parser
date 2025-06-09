import pandas as pd
import re
from typing import Dict, Set, List
import nltk
from tqdm import tqdm
import logging
from spellchecker import SpellChecker
from src.components.lib_setup import LibResourceManager
from src.components.logfactory import get_logger
from constants import DataParserConstants

# set logger
logger = get_logger(__name__)

# Download resources of NLTK
LibResourceManager.download_nlp_resource(['stopwords', 'words'])
# REMOVE_WORDS = DataParserConstants.REMOVED_ENGLISH_WORDS

class DataTransformation:
    def __init__(self):
        self.stopwords:Set = LibResourceManager.get_custom_stopwords(
            add_stop_words=DataParserConstants.REMOVED_ENGLISH_WORDS,
            remove_words=DataParserConstants.DISCARD_STOPWORD)
        self.english_words = set(nltk.corpus.words.words())
        self.banking_keywords = LibResourceManager.get_custom_banking_keywords(
            keyword_dir=DataParserConstants.BANKING_KEYWORD_URL
            )
        self.spellcheck = SpellChecker()

    
    def is_stop_word(self, token):
        # logger.debug(f'Checking in stopwords: {token}')

        if not token or not isinstance(token, str):
            return False

        # Return False if token is empty
        if not token:
            return False

        # Convert to lowercase for comparison
        token_lower = token.strip()

        # If token not in stopwords and length <= 2, return False
        if token_lower not in self.stopwords and len(token) <= 2:
            return False

        # Return True if token is in stopwords
        return token_lower in self.stopwords


    def is_banking_keyword(self, token):
        """ This functions checks if the token/word is exact match within the list of keywords of banking.
            Func:   It also extracts the only alphabetic character and 
                    removes if any numeric character is present or not 
            Args: token: **word** (str)
            Return: Boolean True / False
        """
        # token = re.sub(r'[^A-Za-z]', '', token)
        if token.lower() in self.banking_keywords:
            return token
        else:
            return None
        
    def is_meaningful(self, token):
        """
        Checks if the token is a meaningful English word.
        Only considers alphabetic tokens longer than 1 character.
        """
        if not token or not isinstance(token, str):
            return False

        # Keep only alphabetic characters
        # token = re.sub(r'[^A-Za-z]', '', token)

        # Skip if token is empty or too short
        if not token or len(token) <= 1:
            return False

        return token.lower() in self.english_words

    
    def is_banking_keyword_with_corrected_spell(self, token):
        """
        Corrects the token using spellchecker and checks if it's a banking keyword.
        """
        # spell_check = SpellChecker()
        # spell_check.word_frequency.load_words(self.banking_keywords)

        # token = re.sub(r'[^A-Za-z]', '', token)
        if not token:
            # logger.debug('Token became empty after cleaning, skipping.')
            return None

        corrected_word = self.spellcheck.correction(token)
        if corrected_word and corrected_word.lower() in self.banking_keywords:
            # logger.debug(f'Corrected word: `{corrected_word}` for original token: `{token}`')
            return corrected_word

        return None

    
    def get_embedded_word(self, clean_token):
        """Checks if the token has any embedded form of a banking keyword or vice versa."""
        # clean_token = re.sub(r'[^A-Za-z]', '', token).lower()
        if not clean_token:
            return None

        for keyword in self.banking_keywords:
            keyword_lower = keyword.lower()

            if keyword_lower in clean_token or clean_token in keyword_lower:
                # logger.debug(f'Matched embedded keyword: `{keyword}` with token `{clean_token}`')
                return keyword

        return None



    def is_alphanumeric_with_letters_and_numbers(self, text):
        try:
            if text is None:
                return False  # None is not valid input
            
            if not isinstance(text, str):
                return False  # Only strings are valid inputs
            
            if not text.isalnum():
                return False  # contains non-alphanumeric characters
            
            if text.isdigit():
                digits_count = sum(c.isdigit() for c in text)
                if digits_count>3:

                # Return True if string is only numeric (any length)
                    return True
            
            letters_count = sum(c.isalpha() for c in text)
            digits_count = sum(c.isdigit() for c in text)
            
            # Return True if letters >= 1 and digits > 3
            return letters_count >= 1 and digits_count > 3
        
        except Exception as e:
            # Optional: log the error
            # print(f"Error occurred: {e}")
            return False

    
    def get_transaction_id(self, text):
        parts = re.split(r'[#*,-/]', text)
        parts = [p.strip() for p in parts if p.strip()]
        # logger.debug(f'Splitted Narration: {parts}')

        pattern_ifsc = r'[A-Z]{4}0[A-Z0-9]{6}'
        # reversed_tokens = list(enumerate(reversed(parts)))  # keep index for position tracking
        if parts:
            for word in parts:
                if re.match(pattern_ifsc,word):
                    continue
                if self.is_alphanumeric_with_letters_and_numbers(word):
                    clean_word = re.sub(r'\D', '', word)
                    return clean_word
        return None
                
    def particular_scrapper(self, text):
        parts = re.split(r'[*,-/]', text)
        parts = [p.strip() for p in parts if p.strip()]
        # logger.debug(f'Splitted Narration: {parts}')

        reversed_tokens = list(enumerate(reversed(parts)))  # keep index for position tracking
        
        BANKING_CHANNEL_KEYWORDS = {'UPI', 'NEFT', 'IMPS', }

        try:
            # First pass: banking-related and corrected/embedded keywords
            for rev_index, word in reversed_tokens:
                original_index = len(parts) - 1 - rev_index
                # logger.debug(f'Checking for Keyword in `{word.lower()}` at index {original_index}')

                if not word or not isinstance(word, str):
                    # logger.debug('Empty or invalid word found, skipping.')
                    continue

                clean_word = re.sub(r'[^A-Za-z]', '', word).lower()
                if not clean_word:
                    # logger.debug(f'Word `{word}` reduced to empty after cleaning, skipping.')
                    continue

                if self.is_stop_word(clean_word):
                    # logger.debug(f'Skipping stopword: `{clean_word}`')
                    continue

                # Restrict UPI/NEFT/IMPS to index 0
                contains_channel_keyword = any(keyword in word for keyword in BANKING_CHANNEL_KEYWORDS)
                if contains_channel_keyword:
                    if original_index == 0:
                        is_found_in_banking = self.is_banking_keyword(clean_word)
                        if is_found_in_banking:
                            # logger.debug(f'Found banking keyword `{word}` at position 0')
                            return is_found_in_banking.upper()

                    else:
                        # logger.debug(f'Skipping `{word}` containing banking keyword outside position 0')
                        continue
                else:
                    # Proceed normally for other keywords
                    is_found_in_banking = self.is_banking_keyword(clean_word)
                    if is_found_in_banking:
                        # logger.debug(f'Found in `is_banking`')
                        return is_found_in_banking.upper()


                corrected_word = self.is_banking_keyword_with_corrected_spell(clean_word)
                if corrected_word:
                    # logger.debug(f'Found in `is_banking_keyword_with_corrected_spell`')
                    return corrected_word.upper()

                embedded_word = self.get_embedded_word(clean_word)
                if embedded_word:
                    # logger.debug(f'`{word}` Found in `embedded`')
                    return embedded_word.upper()

            # Second pass: fallback to is_meaningful
            for _, word in reversed_tokens:
                clean_word = re.sub(r'[^A-Za-z]', '', word)
                if not clean_word:
                    continue
                if self.is_stop_word(clean_word):
                    continue
                embedded_word = self.get_embedded_word(clean_word)
                if embedded_word:
                    # logger.debug(f'`{word}` Found in `embedded`')
                    return embedded_word.upper()
                if self.is_meaningful(clean_word):
                    # logger.debug(f'Found in `is_meaningful`')
                    return word.upper()

        except Exception as e:
            logger.error(f'Categorization error occurred: {e}', exc_info=True)

    def category_mapper(self, particular: str):
        mapper_dict = LibResourceManager.get_category_to_spectrum_mapper(
            keyword_dir=DataParserConstants.BANKING_KEYWORD_URL
        )
        # logger.debug(f'Mapping Particular: `{particular}` to Category using mapper_dict: {mapper_dict}')
        if not mapper_dict:
            logger.warning("No category mapping found, returning empty dictionary.")
        
        if particular is None:
            # logger.debug('Particular is None, returning None')
            return None
        
        for scrap_particular, category in mapper_dict.items():
            # logger.info(f'Checking : `{scrap_particular}`')
            if particular.lower() in scrap_particular or scrap_particular in particular.lower():
                # logger.info(f'Category found: {category} for Particular: {particular}')
                return category
        return None


