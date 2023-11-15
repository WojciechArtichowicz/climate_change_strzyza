import requests 
from bs4 import BeautifulSoup
import re
import zipfile  
import os 
from tqdm import tqdm 
import pandas as pd

def clean_directories(main_directory:str)->None:

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

    if os.path.exists(root_dir):        
        for d in dirs_to_create:
            print("Creating {} directory".format(d))
            os.mkdir(os.path.join(root_dir,d))        
    else:
        print("Root directory {} doesn't exist. Aborting.".format(root_dir))



def download_url(url, save_path, chunk_size=128):
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


def unzip_all(zip_dir,unzip_dir):
    for fname in tqdm(os.listdir(zip_dir)):
        if fname.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(zip_dir,fname), 'r') as zr:
                zr.extractall(unzip_dir)


def merge_imgw_csv_files(csv_dir:str,headers:list[str]=None,f_name_pattern:str=None)->pd.DataFrame:
    csv_files = []
    for fname in tqdm(os.listdir(csv_dir)):
        if fname.endswith(".csv"):
            if f_name_pattern is None or re.match(f_name_pattern, fname):
                csv_files.append( pd.read_csv(os.path.join(csv_dir,fname), sep=",", header = None, index_col=False, decimal=".", encoding = "windows-1250") )

    df = pd.concat(csv_files)

    if headers is not None:
        df.columns = headers

    return df




def get_table_from_imgw(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup.find('table')

def get_dates_from_table(bs4_table):

    return  list(map(lambda e: e.replace("/",""),
                     filter(lambda seq: (lambda p,seq: True if re.match(p, seq) else False)(r"^\d{4}\/$|^\d{4}\_\d{4}\/$",seq),
                            map(lambda e: e.string,
                                bs4_table.find_all('a')
                                )
                           )
                    )
                )

def get_info_files_from_table(bs4_table):

    return  list(map(lambda e: e.replace("/",""),
                     filter(lambda seq: (lambda p,seq: True if re.match(p, seq) else False)(r"(\w+).txt$",seq),
                            map(lambda e: e.string,
                                bs4_table.find_all('a')
                                )
                           )
                    )
                )

def download_imgw_info_files(url:str,file_names:str,download_dir:str)->None:

    for fname in file_names:
        download_url(url + "/" + fname, 
                     os.path.join(download_dir, fname)) 

def download_all_zip_files(url:str,date_directories:list[str],file_pattern,download_dir,ext:str=".zip")->None:

    for dd in tqdm(date_directories):
        fname = dd + file_pattern+ext
        full_path_to_file = url + dd + "/" + fname 
        #print(full_path_to_file)
        download_url(full_path_to_file, os.path.join(download_dir, fname)) 


def get_headers_from_info_file(info_file_path:str)->list[str]:
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