# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 13:28:17 2019

@author: Cameron "Who's da Man" Fincher

Updated Thur Jun 24 16:00:00 2021

@author: Mannar Maniraj


added yb tool paths for the new VMs

Updated Thur Jun 27 22:30:00 2021

udpated code to output dataframe with hearders  

Modified 2026-03-19: Added psycopg2 fallback when ybtools unavailable
"""
from os import getcwd, environ, path, remove
from subprocess import check_call, check_output
from datetime import datetime
from sys import platform

from pandas import concat as pd_concat
from pandas import read_csv as pd_read_csv
from pandas import read_sql as pd_read_sql
from glob import glob
import psycopg2


def _psycopg2_unload_fallback(userid: str, passwd: str, table_name: str, 
                               database: str, verbose: bool):
    """
    Fallback method using psycopg2 and pandas when ybtools is unavailable.
    Simply reads the entire table into a DataFrame.
    """
    if verbose:
        print("ybtools not available, using psycopg2/pandas fallback for unload\n")
    
    conn = None
    try:
        conn = psycopg2.connect(
            user=userid,
            password=passwd,
            host="orlpybvip01.catmktg.com",
            port="5432",
            database=database,
        )
        
        if verbose:
            print(f"Reading table '{table_name}' via psycopg2...\n")
        
        df = pd_read_sql(f"SELECT * FROM {table_name}", conn)
        
        if verbose:
            print(f"Successfully read {len(df)} rows from {table_name}\n")
        
        return df
        
    except Exception as e:
        error_msg = str(e)
        if "WLM row limit" in error_msg or "5000000" in error_msg:
            print("\n" + "="*80)
            print("ERROR: Yellowbrick WLM Row Limit Exceeded (5,000,000 rows)")
            print("="*80)
            print(f"\nTable '{table_name}' exceeds the 5 million row limit enforced by")
            print("Yellowbrick's Workload Management (WLM) system.")
            print("\nThis limit applies to Docker/psycopg2 connections but NOT to the")
            print("native ybunload tool used by the Python notebook on Windows.")
            print("\nRECOMMENDED ACTION:")
            print("  Run this request manually using the VMR Scorecard Python notebook")
            print("  located in the 'Reports' folder on your local machine.")
            print("  The notebook uses ybunload.exe which bypasses WLM limits.")
            print("="*80 + "\n")
        else:
            print(f"psycopg2 fallback unload failed: {e}")
        return None
    finally:
        if conn:
            conn.close()

        
def yb_unload(userid: str,
              passwd: str,
              table_name: str,
              database: str = 'py1usta1',
              save_dir: str =  None,
              delete_files: bool = True,
              delim: str = '|',
              yb_params: str = '',
              ybtools_path: str = None,
              verbose: bool = True):
    """ 
    Allows a python users to run a ybunload from Yellowbrick from within 
    python. Return will be a pandas dataframe of the table exported WITH HEADERS. (refer update above)
    None is returned if it erroed out.
    Works with Python 3.x
    
        uerid: Userid to yellowbrick
        passwd: Password to yellowbrick
        table_name: Name of table to unload
        database: The database the table is located in (default py1usta1)
        save_dir: The directory to save the unloaded directory, defualt cwd
        delete_dir: Delete teh files once downloaded
        delim: Delimeter of the saved files
        yb_params: Addtional arguments to be passed to YBUNLOAD, must be a single string
        ybtools_path: the directory where ybtools are located, will check several
            locations depending on the system you are using.
        verbose: Will print each step"""
            
    # Checking the Save location
    if verbose:
        print("1 - Checking that the save location is a valid location\n")
    if save_dir is None:
        save_dir = getcwd()
    
    if not path.isdir(save_dir):
        print("Save Directory provided for yb_unload does not exists")
        return(None)
        
    # Finding which tool set to use depending on the system
    if platform in ['win32','cygwin']:
        yb_software = 'ybunload.exe'
        yb_sql = 'ybsql.exe'
    else:
        yb_software = 'ybunload'
        yb_sql = 'ybsql'
        
    if verbose:
        print("2 - Finding the Ybtools location and validating their existence\n")
    # Determing if the ybtools path exists
    if ybtools_path is None:
        if platform.startswith('linux') and path.isfile('/usr/bin/' + yb_sql):
            #IT insatlled the ybsql tool in an odd location, so I need to check for that
            #if ybsql is there, then so is ybload
            ybtools_path = '/usr/bin/'
        elif platform.startswith('linux') and path.isdir('/sasuser/cmc/ybrick/ybtools/bin/'):
            ybtools_path = '/sasuser/cmc/ybrick/ybtools/bin/'
        elif platform.startswith('linux') and path.isdir('/apps/ybtools/bin/'):
            ybtools_path = '/apps/ybtools/bin/'
        elif platform.startswith('linux') and path.isdir('/opt/ybtools/bin/'):
            ybtools_path = '/opt/ybtools/bin/'
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/ybtools/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/ybtools/Client Tools/bin/'
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/Yellowbrick Data/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/Yellowbrick Data/Client Tools/bin/'
        else:
            # ybtools not found - use psycopg2 fallback
            print("Could not find ybtools in the default location on the system")
            return _psycopg2_unload_fallback(userid, passwd, table_name, database, verbose)
    print(ybtools_path)
    if not path.isdir(ybtools_path):
        print("Could not determine the path where the ybtools are located")
        return _psycopg2_unload_fallback(userid, passwd, table_name, database, verbose)
        
    if not path.isfile(path.join(ybtools_path, yb_software)):
        print("Couldn't find ybunload program on the system.")
        return _psycopg2_unload_fallback(userid, passwd, table_name, database, verbose)
    
    #Setting the prefix
    prefix = 'YBUNLOAD_' + datetime.today().strftime('%Y%m%d%I%M%S')
    #now, putting together the call
    if verbose:
        print("3 - Prepping the YBUNLOAD call \n")
    if platform in ['win32','cygwin']:
        Yb_call1 = '{} -h orlpybvip01.catmktg.com -d {} -A --username {} -w -c {} > {}/{}_header.txt '
    else: 
        Yb_call1 = '{} -h orlpybvip01.catmktg.com -d {} -A --username {} -w -c {} |tr -d "+-" > {}/{}_header.txt '
    Yb_call = "{} -h orlpybvip01.catmktg.com -d {} --username {} --table {}" + \
                " --format text --delimiter {} -o {} --prefix {} --truncate-existing {}" 
    Yb_call = Yb_call.format('\"' + path.join(ybtools_path, yb_software) + '\"', \
                         database, \
                         userid, \
                         table_name, \
                         '\"' +  delim + '\"',
                         '\"' + save_dir + '\"', \
                         prefix,
                         yb_params
                         )
    # Making the call
    try:
        if verbose:
            print("4 - Making the YBUNLOAD call with this statement\n")
            print("\t\t %s" % Yb_call)
        #https://blog.miguelgrinberg.com/post/how-to-make-python-wait
        environ['YBPASSWORD'] = passwd
        #making the call
        CLI_CALL = check_call(Yb_call, shell = True)
        
        SQL_CALL = 'select * from {} limit 0'.format(table_name)
        Yb_drop = Yb_call1.format('\"' + path.join(ybtools_path, yb_sql) + '\"', \
                         database, \
                         userid, \
                         '\"' + SQL_CALL  +'\"', 
                          save_dir,
                         prefix)
            
        environ['YBPASSWORD'] = passwd
        print(Yb_drop)
        #making the call
        print(check_output(Yb_drop,  shell = True)) #creating a file with headers
        if CLI_CALL != 0:
            del environ['YBPASSWORD']
            print('Error: Something went wront with the command line call: Here is what was passed to the call')
            print('\n\n ****************************************** \n')
            print(Yb_call)
            return(None)
            
    except Exception as e: 
        del environ['YBPASSWORD']
        print(e)
        return(None)
    
    #Pulling in all files from the ybunload now:
    file_list_in = glob(str(path.join(save_dir,prefix) + '*.txt'))
    if verbose:
        print("5 - Reading in %s file(s) created from YBUNLOAD\n" % len(file_list_in))
    import_list = []
    Error_flag = False
    try:
        for filename in file_list_in:
            if '_header.txt' in filename:
                column_nm = pd_read_csv(filename, index_col=None, header=0, sep =delim).columns
            else:
                df = pd_read_csv(filename, index_col=None, header=None, sep =delim)
                import_list.append(df)
        df = pd_concat(import_list, axis =0, ignore_index = True, sort = False)
        df.columns = column_nm
    except Exception as e: 
        print(e)
        Error_flag = True
    
    #removing files:
    if delete_files:
        for filename in file_list_in:
            remove(filename) 
        
    if Error_flag:
        return(None)
    else:
        return(df)


def yb_unload_files(userid: str,
              passwd: str,
              table_name: str,
              database: str = 'py1usta1',
              save_dir: str =  None,
              delim: str = '|',
              yb_params: str = '',
              ybtools_path: str = None,
              verbose: bool = True):
    """ 
    Allows a python users to run a ybunload from Yellowbrick from within 
    python. Will return TRUE if ran success or FALSE otherwaise.
    Works with Python 3.x
    
        uerid: Userid to yellowbrick
        passwd: Password to yellowbrick
        table_name: Name of table to unload
        database: The database the table is located in (default py1usta1)
        save_dir: The directory to save the unloaded directory, defualt cwd
        delim: Delimeter of the saved files
        yb_params: Addtional arguments to be passed to YBUNLOAD, must be a single string
        ybtools_path: the directory where ybtools are located, will check several
            locations depending on the system you are using.
        verbose: Will print each step"""
            
    # Checking the Save location
    if verbose:
        print("1 - Checking that the save location is a valid location\n")
    if save_dir is None:
        save_dir = getcwd()
    
    if not path.isdir(save_dir):
        print("Save Directory provided for yb_unload does not exists")
        return(False)
        
    # Finding which tool set to use depending on the system
    if platform in ['win32','cygwin']:
        yb_software = 'ybunload.exe'
        yb_sql = 'ybsql.exe'
    else:
        yb_software = 'ybunload'
        yb_sql = 'ybsql'
        
    if verbose:
        print("2 - Finding the Ybtools location and validating their existence\n")
    # Determing if the ybtools path exists
    if ybtools_path is None:
        if platform.startswith('linux') and path.isfile('/usr/bin/' + yb_sql):
            #IT insatlled the ybsql tool in an odd location, so I need to check for that
            #if ybsql is there, then so is ybload
            ybtools_path = '/usr/bin/'
        elif platform.startswith('linux') and path.isdir('/sasuser/cmc/ybrick/ybtools/bin/'):
            ybtools_path = '/sasuser/cmc/ybrick/ybtools/bin/'
        elif platform.startswith('linux') and path.isdir('/apps/ybtools/bin/'):
            ybtools_path = '/apps/ybtools/bin/'
        elif platform.startswith('linux') and path.isdir('/opt/ybtools/bin/'):
            ybtools_path = '/opt/ybtools/bin/'
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/ybtools/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/ybtools/Client Tools/bin/'
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/Yellowbrick Data/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/Yellowbrick Data/Client Tools/bin/'
        else:
            # Mac is scipped as there is no defualt location currently
            print("Could not find ybtools in the default location on the system")
            return(False)
    
    if not path.isdir(ybtools_path):
        print("Could not determine the path where the ybtools are located")
        return(False)
        
    if not path.isfile(path.join(ybtools_path, yb_software)):
        print("Couldn't find ybunload program on the system.")
        return(False)
    
    #Setting the prefix
    prefix = table_name + '_' + datetime.today().strftime('%Y-%m-%d_')
    #now, putting together the call
    if verbose:
        print("3 - Prepping the YBUNLOAD call \n")
    Yb_call = "{} -h orlpybvip01.catmktg.com -d {} --username {} --table {}" + \
                " --format text --delimiter {} -o {} --prefix {} --truncate-existing {}" 
    Yb_call = Yb_call.format('\"' + path.join(ybtools_path, yb_software) + '\"', \
                         database, \
                         userid, \
                         table_name, \
                         '\"' +  delim + '\"',
                         '\"' + save_dir + '\"', \
                         prefix,
                         yb_params
                         )
    # Making the call
    try:
        if verbose:
            print("4 - Making the YBUNLOAD call\n")
        environ['YBPASSWORD'] = passwd
        #making the call
        CLI_CALL = check_call(Yb_call, shell = True)
        
        if CLI_CALL != 0:
            del environ['YBPASSWORD']
            print('Error: Something went wront with the command line call: Here is what was passed to the call')
            print('\n\n ****************************************** \n')
            print(Yb_call)
            return(False)
            
    except Exception as e: 
        del environ['YBPASSWORD']
        print(e)
        return(False)
    
    return(True)