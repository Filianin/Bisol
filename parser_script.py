import os
import re
from datetime import datetime
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd


# Set up logging
log_filename = 'error.log'

# The save directory folder of the log should be replaced with a real save path
log_folder_path = 'logs'

# Create the log folder if it doesn't exist
os.makedirs(log_folder_path, exist_ok=True)

log_file_path = os.path.join(log_folder_path, log_filename)

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)


def save_xml(url, save_directory):
    """
    Downloads an XML file from the given URL and saves it to the specified directory.

    Args:
        url (str): The URL of the XML file to download.
        save_directory (str): The directory where the downloaded XML file will be saved.

    Returns:
        None

    Raises:
        None

    """
    try:
        # Get the content of the provided web page
        response = requests.get(url)
        
        if response.status_code == 200:
            # Extract the desired part from the URL using regex
            regex = r'/observationAms_(.*?)_history\.xml'
            match = re.search(regex, url)
            
            # Get the current datetime and format it as 'YYYY_MM_DD_HH_MM'
            current_datetime = datetime.now()
            formatted_datetime = current_datetime.strftime("%Y_%m_%d_%H_%M")
            
            # Create the filename using the formatted datetime and the extracted part from the URL
            filename = formatted_datetime + '_' + match.group(1) + '.xml'
            
            # Create the save path by joining the save_directory and filename
            save_path = os.path.join(save_directory, filename)
            
            # Save the XML content to the specified file path
            with open(save_path, 'wb') as file:
                file.write(response.content)
    
    except Exception as e:
        logging.exception(f"Error occured during saving XML file from {url}: {e}")
            

def get_list_of_links():
    """
    Retrieves a list of XML file links from a web page and returns them as a list.

    Returns:
        list: A list of XML file links.

    Raises:
        None

    """
    try:
        # Get the content of the main web page
        source = 'https://meteo.arso.gov.si/met/sl/service/'
        page = requests.get(source)
    
        # Parse the main web page using BeautifulSoup
        soup = BeautifulSoup(page.content, "html.parser")
        iframe = soup.find('iframe')
        iframe_src = iframe['src']
        
        # Define a prefix for the URL complement
        url_prefix = 'https://meteo.arso.gov.si'
        
        # Retrieve the content of the iframe source URL
        response = requests.get(url_prefix + iframe_src)
        iframe_soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the tables with the specified class and ID
        tables = iframe_soup.find_all('table', class_='meteoSI-table', id='observe')
        
        data = []
        
        # Iterate over the rows of the third table starting from the 4th row
        for row in tables[2].find_all('tr')[3:]:
            row_data = []
            
            # Iterate over the cells of each row
            for cell in row.find_all('td'):
                # If the cell contains an 'a' tag
                if cell.find('a'):
                    a_tag = cell.find('a')
                    if 'href' in a_tag.attrs:
                        row_data.append(a_tag['href'])
                    else:
                        row_data.append(a_tag.text.strip())
                else:
                    row_data.append(cell.text.strip())
            
            # Add the row data to the list
            data.append(row_data)
        
        # Create a DataFrame from the extracted data
        df = pd.DataFrame(data)
        
        # Delete the columns RSS and HTML, rename the remaining columns
        df = df.drop([2, 3], axis=1).rename(columns={0:'location', 1:'XML_latest'})
        
        # Extract the location from the 'XML_latest' column using regex
        regex_pattern = r'/observationAms_(.*?)_latest\.xml'
        df['location'] = df['XML_latest'].str.extract(regex_pattern)
        
        # Create the 'XML_history' column by appending the location to the history URL pattern
        df['XML_history'] = 'http://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/recent/observationAms_' \
                            + df['location'] + '_history.xml'
        
        # Return the 'XML_history' column values as a list
        return df['XML_history'].to_list()
    
    except Exception as e:
        logging.exception(f"Error occurred while retrieving list of links: {e}")
        return []


def parse_xml():
    """
    Parses XML files from a list of URLs and saves them locally in a directory 
    called 'XMLs'.

    Returns:
        None

    Raises:
        None

    """
    try:
        # Get the directory path of the current script file
        #script_directory = os.path.dirname(os.path.realpath(__file__))
    
        # Define the save directory path
        # The save directory path of the script should be replaced with a real save
        # path - a data lake or AWS S3 or similar
        # Get the absolute path of the script file
        script_path = os.path.abspath(__file__)
        script_directory = os.path.dirname(script_path)
        save_directory = os.path.join(script_directory, 'XMLs')
    
        # Create the save directory if it doesn't exist
        os.makedirs(save_directory, exist_ok=True)
    
        # Iterate over each URL in the list of links
        for url in get_list_of_links():
            # Save the XML file locally in the 'XMLs' directory
            save_xml(url, save_directory)
    
    except Exception as e:
        logging.exception(f"Error occurred during XML parsing and saving: {e}")


if __name__ == "__main__":
    try:
        parse_xml()
    except Exception as e:
        logging.exception(f"Unhandled error: {e}")