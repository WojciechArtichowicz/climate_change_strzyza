import requests 
from bs4 import BeautifulSoup
import re
import zipfile  
import os 
from tqdm import tqdm 
import pandas as pd

def clean_directories(main_directory:str)->None:
    """Function cleans the given directory: removes all files and directories in the given root dir.

    Parameters
    ----------
    main_directory : str
        A directory to be cleaned. Must exist. 

    Returns
    -------
    None  
    """

    if os.path.exists(main_directory):
        print("path exists - removing contents of {}".format(main_directory))

        for root, dirs, files in os.walk(main_directory, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
                
            for d in dirs:
                os.rmdir(os.path.join(root, d))
    else:
        print("Path {} doesn't exist. This folder has to be created manually.".format(main_directory))


def create_directories(root_dir:str,dirs_to_create:list[str])->None:
    """Function creates directories in a given root directory. Root directory needs to exist.
    
    Parameters
    ----------
    root_dir : str
        A directory in which listed directories are to be created

    dirs_to_create : list[str]
        A list of directories to be created inside the root_dir
    """

    if os.path.exists(root_dir):        
        for d in dirs_to_create:
            print("Creating {} directory".format(d))
            os.mkdir(os.path.join(root_dir,d))        
    else:
        print("Root directory {} doesn't exist. Aborting.".format(root_dir))



def download_url(url:str, save_path:str, chunk_size:int=128)->None:
    """Code taken from the requests doc website: https://requests.readthedocs.io/en/latest/user/quickstart/#raw-response-content
    Function takes an url of the resource and a path on a local computer and downloads the resource to a given specification. 

    Parameters
    ----------
    url : str
        Link to the resource to be downloaded

    save_path : str
        Path where the resource is to be saved

    chunk_size : int, optional
        Size of the downloaded chunk (default 128b)
    """

    r = requests.get(url, stream=True)
    if r.status_code == 200:
        try:
            with open(save_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
        except:
            print("Unable to download the file.")
    else:
        print("File does not exist. Is URL proper? Unable to download the file.")


def unzip_all(zip_dir:str,unzip_dir:str)->None:
    """Function unzips all the *.zip files in a given directory (zip_dir) and extracts them to a given directory unzip_dir
    
    Parameters
    ----------
    zip_dir : str
        Directory where the zip files are stored
    
    unzip_dir : str
        Directory where the zip files are to be extracted
    """

    for fname in tqdm(os.listdir(zip_dir)):
        if fname.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(zip_dir,fname), 'r') as zr:
                zr.extractall(unzip_dir)


def merge_imgw_csv_files(csv_dir:str,headers:list[str]=None,f_name_pattern:str=None)->pd.DataFrame:
    """Function merges (concatenates) all the IMGW csv files into one pandas dataframe.
    
    Parameters
    ----------
    csv_dir : str
        Directory containing the IMGW csv files
    
    headers : list[str], optional
        Headers of the resulting data frame (default is None)

    f_name_pattern : str, optional
        Name pattern of the IMGW csv files. Different types of IMGW data have different 
        file namings. This parameter allows to distinguish them easily.

    Returns
    -------
        Pandas data frame containing the data from all IMGW csv files with a given name pattern.
    """

    csv_files = []
    for fname in tqdm(os.listdir(csv_dir)):
        if fname.endswith(".csv"):
            if f_name_pattern is None or re.match(f_name_pattern, fname):
                csv_files.append( pd.read_csv(os.path.join(csv_dir,fname), sep=",", header = None, index_col=False, decimal=".", encoding = "windows-1250") )

    df = pd.concat(csv_files)

    if headers is not None:
        df.columns = headers

    return df




def get_table_from_imgw(url:str)->BeautifulSoup:
    """Function gets the raw table object from the IMGW data website. For example:
    https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/
    Used to get the list of folders with data.
    
    url : str
        Address of the website with the table of directories and IMGW info files

    Returns
    -------
    table : BeautifulSoup.element.Tag
        Extracted table from html
    """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup.find('table')

def get_dates_from_table(bs4_table:BeautifulSoup)->list[str]:
    """Function parses the list of dates (data directories) from the table present 
    on the IMGW website (example): 
    https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/
    
    Parameters
    ----------
    s4_table : str
        Beautiful soup table taken from the IMGW website.

    Returns
    -------
    dates : list[str]
        List of directories containing the data files
    """
    
    return  list(map(lambda e: e.replace("/",""),
                     filter(lambda seq: (lambda p,seq: True if re.match(p, seq) else False)(r"^\d{4}\/$|^\d{4}\_\d{4}\/$",seq),
                            map(lambda e: e.string,
                                bs4_table.find_all('a')
                                )
                           )
                    )
                )

def get_info_files_from_table(bs4_table:BeautifulSoup)->list[str]:
    """Function parses the list of info files from the table present 
    on the IMGW website (example): 
    https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/
    
    Parameters
    ----------
    s4_table : str
        Beautiful soup table taken from the IMGW website.

    Returns
    -------
    info_file_names : list[str]
        List of info files present in the table
    """

    return  list(map(lambda e: e.replace("/",""),
                     filter(lambda seq: (lambda p,seq: True if re.match(p, seq) else False)(r"(\w+).txt$",seq),
                            map(lambda e: e.string,
                                bs4_table.find_all('a')
                                )
                           )
                    )
                )

def download_imgw_info_files(url:str,file_names:list[str],download_dir:str)->None:
    """Function downloades the data info file from the IMGW website
    
    Parameters
    ----------
    url : str
        The link to the main resource (without the file names)
    
    file_names : list[str]
        List of the file names to download (without the full url)
    
    download_dir : str
        Local directory where the files are to be downloaded
    """

    for fname in file_names:
        download_url(url + "/" + fname, 
                     os.path.join(download_dir, fname)) 

def download_all_zip_files(url:str,date_directories:list[str],file_pattern:str,download_dir:str,ext:str=".zip")->None:
    """Downloads all the data (zip) files from the given IMGW data website.

    Parameters
    ----------
    url : str
        Link to the resource (main www, without the file names)

    date_directories : list[str]
        List of directories from which zip files are to be downloaded

    file_pattern : str
        Different IMGW data files have different naming convention. 
        This parameter allows to easily set the file naming pattern.
        For example on the website https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/
        the file naming pattern is '_m_k' resulting in names like: 'YYYY_m_k.zip'

    download_dir : str
        Directory to which the zip files will be downloaded

    ext : str, optional
        Downloaded files extension (includes period) (default is ".zip"). 
    """
    for dd in tqdm(date_directories):
        fname = dd + file_pattern+ext
        full_path_to_file = url + dd + "/" + fname 
        #print(full_path_to_file)
        download_url(full_path_to_file, os.path.join(download_dir, fname)) 


def get_headers_from_info_file(info_file_path:str)->list[str]:
    """Function extracts the data headers from an IMGW info file.
    
    Parameters
    ----------
    info_file_path : str
        Local machine path to previously downloaded info file
    
    Returns
    -------
    headers : list[str]
        List of headers for pandas dataframes
    """
    # Each imgw info file has a footer. Code below finds the empty line
    # separating the footer
    footer_ix = -1
    with open(info_file_path,encoding="windows-1250") as info_file:
        info_file.readline() # each imgw info file starts with an empty line
        content = info_file.readlines()
        
        footer_ix = list(filter(lambda e:e[-1],enumerate(map(lambda s:re.match(r"(\s+\n)|(\n)",s) is not None,content))))[0][0]
        content = content[:footer_ix] # remove footer
        
        #return content
        # getting headers from an info file
        return list(map(lambda e: tuple(filter(lambda s: not s=='',e[0]))[0], #e[0][0] if e[0][1]=="" else e[0][1], in case it returned more than two elements
                        map(lambda line: 
                            re.findall("(^[A-ZĄĘĆŃŻŹŚĆ].*[A-ZĄĘĆŃŻŹŚĆa-ząęćńżźść%°|]])|(^[\WA-Z].*[A-ZĄĘĆŃŻŹŚĆa-ząęćńżźść])",
                            line,
                            re.UNICODE),
                            content
                            )
                        )
                    )
    # if gets here returns None