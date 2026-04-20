# -*- coding: utf-8 -*-
"""
Created on Sat Nov 1st 13:28:17 2019

@author: Cameron "He is NOT The man" Fincher

Modified 2026-03-19: Added psycopg2 COPY fallback when ybtools unavailable
"""
from os import getcwd, environ, path, remove
from subprocess import check_call
from datetime import datetime
from sys import platform
from re import sub
from io import StringIO

from pandas import DataFrame
import psycopg2


def _psycopg2_fallback(Df: DataFrame, userid: str, passwd: str, table_name: str,
                       append: bool, database: str, verbose: bool) -> bool:
    """
    Fallback method using psycopg2 COPY when ybtools is unavailable.
    Mimics yb_load behavior: removes non-alphanumeric chars from column names.
    """
    if verbose:
        print("ybtools not available, using psycopg2 COPY fallback\n")
    
    # PostgreSQL lowercases unquoted identifiers - be consistent
    table_name = table_name.lower()
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            user=userid,
            password=passwd,
            host="orlpybvip01.catmktg.com",
            port="5432",
            database=database,
        )
        conn.set_session(autocommit=True)
        cursor = conn.cursor()
        
        # Normalize column names (same as ybtools: remove non-alphanumeric)
        normalized_cols = {col: sub(r'[^A-Za-z0-9]+', '', col) for col in Df.columns}
        df_normalized = Df.rename(columns=normalized_cols)
        
        # Drop table if not appending
        if not append:
            if verbose:
                print(f"Dropping table if exists: {table_name}\n")
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            
            # Type mapping (same as original yb_load)
            pd_to_yb = {
                'float64': 'FLOAT',
                'int64': 'BIGINT',
                'object': 'VARCHAR',
                'o': 'VARCHAR',
                'datetime64[ns]': 'Date',
                'float32': 'FLOAT4',
                'bool': 'BOOL',
                'int8': 'INTEGER',
            }
            
            # Build column definitions
            col_defs = []
            for col, dtype in df_normalized.dtypes.items():
                yb_type = pd_to_yb.get(str(dtype), 'VARCHAR')
                if yb_type == 'VARCHAR':
                    if Df.shape[0] < 500000:
                        max_len = df_normalized[col].astype(str).str.len().max()
                    else:
                        max_len = df_normalized[col].iloc[:500000].astype(str).str.len().max()
                    col_defs.append(f'{col} VARCHAR({max(int(max_len) + 10, 50)})')
                else:
                    col_defs.append(f'{col} {yb_type}')
            
            create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)}) DISTRIBUTE RANDOM'
            if verbose:
                print(f"Creating table: {table_name}\n")
                print("Column structure:")
                for i, (col, dtype) in enumerate(df_normalized.dtypes.items()):
                    print(f"\t{col}: {dtype} -> {col_defs[i].split()[1]}")
                print()
            cursor.execute(create_sql)
        
        # Use COPY for fast bulk insert
        if verbose:
            print(f"Loading {len(df_normalized)} rows via psycopg2 COPY...\n")
        
        buffer = StringIO()
        df_normalized.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        cursor.copy_from(buffer, table_name, sep='\t', null='\\N', columns=df_normalized.columns.tolist())
        
        if verbose:
            print(f"Successfully loaded {len(df_normalized)} rows to {table_name}\n")
        
        return True
        
    except Exception as e:
        print(f"psycopg2 fallback failed: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def df_len(c, Df):
    """Helper function to find how many characters varchar needs to hold
    limits it to the first 500,000 records"""
    if str(c[1]) == 'object' and Df.shape[0] < 500000:
        return( '( %i )' % Df[c[0]].astype(str).map(len).max())
    elif str(c[1]) == 'object' and Df.shape[0] >= 500000:
        return( '( %i )' % Df.loc[0:500000,c[0]].astype(str).map(len).max())
    else:
        return('')


def yb_load(Df: DataFrame,
              userid: str,
              passwd: str,
              table_name: str,
              append: bool = False,
              delete_file: bool = True,
              database: str = 'py1usta1',
              yb_params: str = '--max-bad-rows 3',
              delim: str = '|',
              save_path: str =  None,
              ybtools_path: str = None,
              verbose: bool = True) -> bool:
    """ 
    Allows users to pass in a pandas data frame and save it to Yellowbrick.
    will return TRUE if it ran correctly, else will return FALSE
    Works with Python 3.x
        
        Df: pandas data frame
        uerid: Userid to yellowbrick
        passwd: Password to yellowbrick
        table_name: Name of table to load to or create and load too
        append: Should you just append the data (if it already exists) (TRUE) or
                drop table if exists, and recreate it (False)
        delete_file: Delete the file that is saved to the hard drive for ybload?
        database: The database the table is located in (default py1usta1)
        yb_params: Additional paramater to pass to the YBLOAD tool. It has to be in one complete string
        yb_delim: the delimer for the save file, defaults to pipe
        save_path: The directory to save the panadas data frame too, defualt is cwd
        delim: The delimiter used for pandas file that is saved to the drive
        ybtools_path: the directory where ybtools are located, will check several
            locations depending on the system you are using.
        verbose: Will print each step"""
            
    # Checking the Save location
    if verbose:
        print("Checking that the save and YBtools locations\n")
    if save_path is None:
        save_path = getcwd()
    
    if not path.isdir(save_path):
        print("Save Directory provided for yb_load does not exists")
        return(None)
        
    # Finding which tool set to use depending on the system
    if platform in ['win32','cygwin']:
        ybload = 'ybload.exe'
        yb_sql = 'ybsql.exe'
    else:
        ybload = 'ybload'
        yb_sql = 'ybsql'
        
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
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/ybtools/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/ybtools/Client Tools/bin/'
        elif platform in ['win32','cygwin'] and path.isdir('c:/Program Files/Yellowbrick Data/Client Tools/bin/'):
            ybtools_path = 'c:/Program Files/Yellowbrick Data/Client Tools/bin/'
        else:
            # ybtools not found - use psycopg2 fallback
            print("Could not find ybtools in the default location on the system")
            return _psycopg2_fallback(Df, userid, passwd, table_name, append, database, verbose)
    
    if not path.isdir(ybtools_path):
        print("Could not determine the path where the ybtools are located")
        return _psycopg2_fallback(Df, userid, passwd, table_name, append, database, verbose)
        
    if not path.isfile(path.join(ybtools_path, ybload)):
        print("Couldn't find ybload program on the system.")
        return _psycopg2_fallback(Df, userid, passwd, table_name, append, database, verbose)

    if not path.isfile(path.join(ybtools_path, yb_sql)):
        print("Couldn't find ybsql (supporting program) on the system.")
        return _psycopg2_fallback(Df, userid, passwd, table_name, append, database, verbose)
       
    if not isinstance(table_name, str):
        print('Table name is not a string. Please provided a table name')
        return(False)
        
    #setting up the basic ybsql call string, the after the '-c' is wehre the qry goes
    Yb_call = '{} -h orlpybvip01.catmktg.com -d {} --username {} -w -c {}'

    if not append:
        #Execute a drop table if exists
        if verbose:
            print("Checking and removing if a table by the same name exists\n\n")
            
        try:
            SQL_CALL = 'drop table if exists {}'.format(table_name)
            Yb_drop = Yb_call.format('\"' + path.join(ybtools_path, yb_sql) + '\"', \
                         database, \
                         userid, \
                         '\"' + SQL_CALL  +'\"')
            
            environ['YBPASSWORD'] = passwd
            #making the call
            # Sehll  = TRUE is not secure....I KNOW!
            CLI_CALL = check_call(Yb_drop, shell = True)
            
            if CLI_CALL != 0:
                del environ['YBPASSWORD']
                print('Error: Something went wront with the command line call: Here is what was passed to the call')
                print('\n\n ****************************************** \n')
                print(Yb_drop)
                return(False)
                
        except Exception as e: 
            del environ['YBPASSWORD']
            print(e)
            return(False)
    # Creating table if not exists, again both append and not append use this
    #creating the table now
    if verbose:
        if append:
            print("Creating table if it ain't already there\n\n")
        else:
            print("Creating table\n\n")

    
    pd_to_yb = {'float64':'FLOAT',
                'int64':'BIGINT',
                'object':'VARCHAR',
                'o':'VARCHAR',
                'datetime64[ns]':'Date',
                '<MB[ns]':'Date',
                'float32':"FLOAT4",
                "bool":"BOOL",
                "int8":"INTEGER"}
    
            
    FIELDS =  [(sub('[^A-Za-z0-9]+', '', c[0]) ,pd_to_yb[str(c[1])] + df_len(c, Df)) for c in zip(Df.columns, Df.dtypes)]
   
    if not append and verbose:
       print( "Here is your new table structure\n\n")
       print("***************************************************************\n")
       print("\t FIELD: \t Pandas -> YB \n")
       for i in range(Df.shape[1]):
           print("\t %s: \t %s -> %s" % (FIELDS[i][0],str(Df.dtypes[i]),FIELDS[i][1] ))
       print("***************************************************************\n")
        
    # now actually creatibng the table

    try:
         SQL_CALL = 'create table if not exists {} ( {} ) distribute random'.format(table_name,
                                              ','.join([c[0] +' ' +c[1] for c in FIELDS]))
         Yb_make = Yb_call.format('\"' + path.join(ybtools_path, yb_sql) + '\"', \
                                     database, \
                                     userid, \
                                     '\"' + SQL_CALL  +'\"')
            
         environ['YBPASSWORD'] = passwd
         #making the call
         # Shell  = TRUE is not secure....I KNOW!
         CLI_CALL = check_call(Yb_make, shell = True)
            
         if CLI_CALL != 0:
            del environ['YBPASSWORD']
            print('Error: Something went wront with the command line call: Here is what was passed to the call')
            print('\n\n ****************************************** \n')
            print(Yb_make)
            return(False)
                
    except Exception as e: 
        del environ['YBPASSWORD']
        print(e)
        return(False)


    #Now saving the pandas file out there
    if verbose:
        print("Saving data frame to the drive for loading into YB\n")
    try:
        postfix = datetime.today().strftime('%Y-%m-%d-%I%M') 
    except:
        try:
            postfix = datetime.date.today().strftime('%Y-%m-%d-%I%M')
        except:
            postfix = '2001_a_space_odyssey'
    LOG_FILE = path.join(save_path, 'ybload_log-' + str(postfix) + '.txt')
    BAD_FILE = path.join(save_path, 'ybload_badrows-' + str(postfix) + '.txt')
    SAVE_FILE = path.join(save_path, table_name + '-' + str(postfix) + '.txt')
    
    Df.to_csv(SAVE_FILE, sep = delim,index = False)
    # Making the call

    try:

        Yb_load_call = '{} -h orlpybvip01.catmktg.com -d {} --username {} --table {}' + \
        " --format TEXT --delimiter {} --num-header-lines 1 {} --logfile {} --logfile-log-level INFO " + \
        '--bad-row-file {} {}'
        Yb_load_call = Yb_load_call.format('\"' + path.join(ybtools_path, ybload) + '\"',
                                                 database, 
                                                 userid, 
                                                 table_name, 
                                                 '\"' +  delim + '\"' ,
                                                 '\"' + SAVE_FILE + '\"',
                                                 '\"' + LOG_FILE + '\"',
                                                 '\"' + BAD_FILE + '\"',
                                                 yb_params)

        if verbose:
            print("Bulk loading has commenced\n")
        #https://blog.miguelgrinberg.com/post/how-to-make-python-wait
        environ['YBPASSWORD'] = passwd
        #making the call
        CLI_CALL = check_call(Yb_load_call, shell = True)
        
        if CLI_CALL != 0:
            del environ['YBPASSWORD']
            print('Error: Something went wront with the bulk load: Here is what was passed to the call')
            print('\n\n ****************************************** \n')
            print(Yb_load_call)
            print('\n\n ****************************************** \n')
            print("There is an error file here: %s \nThere is a log file here %s" % (BAD_FILE,LOG_FILE))
            return(None)
            
    except Exception as e: 
        del environ['YBPASSWORD']
        print(e)
        print('\n\n ****************************************** \n')
        print("There is an error file here: %s \nThere is a log file here %s" % (BAD_FILE,LOG_FILE))
        return(None)
    try:
        remove(LOG_FILE)
        remove(BAD_FILE)
    except:
        pass
    
    if delete_file:
        if verbose:
            print("removing pandas saved file")
        remove(SAVE_FILE)
        

    
    return(True)
      


def yb_load_file(file_path: str,
              userid: str,
              passwd: str,
              table_name: str,
              delete_file: bool = False,
              database: str = 'py1usta1',
              yb_params: str = '--max-bad-rows 3 --num-header-lines 1',
              delim: str = '|',
              ybtools_path: str = None,
              verbose: bool = True) -> bool:
    """ 
    Allows users to pass in a pandas data frame and save it to Yellowbrick.
    will return TRUE if it ran correctly, else will return FALSE
    Works with Python 3.x
        
        file_path: File to load into yellowbrick
        uerid: Userid to yellowbrick
        passwd: Password to yellowbrick
        table_name: Name of table to load to
        delete_file: Delete the file that is saved to the hard drive for ybload?
        database: The database the table is located in (default py1usta1)
        yb_params: Additonal paramaters to pass to ybload.
        delim: The delimiter used for pandas file that is saved to the drive
        ybtools_path: the directory where ybtools are located, will check several
            locations depending on the system you are using.
        verbose: Will print each step"""
            
    # Checking the Save location
    if verbose:
        print("Checking that the save and YBtools locations\n")
    
    if not path.isfile(file_path):
        print("Save Directory provided for yb_load does not exists")
        return(None)
    else:
        save_path = path.dirname(file_path)
        
    # Finding which tool set to use depending on the system
    if platform in ['win32','cygwin']:
        ybload = 'ybload.exe'
        yb_sql = 'ybsql.exe'
    else:
        ybload = 'ybload'
        yb_sql = 'ybsql'
        
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
        
    if not path.isfile(path.join(ybtools_path, ybload)):
        print("Couldn't find ybload program on the system.")
        return(False)

    postfix = datetime.today().strftime('%Y-%m-%d-%I%M') 
    LOG_FILE = path.join(save_path, 'ybload_log-' + str(postfix) + '.txt')
    BAD_FILE = path.join(save_path, 'ybload_badrows-' + str(postfix) + '.txt')
    
    # Making the call

    try:

        Yb_load_call = '{} -h orlpybvip01.catmktg.com -d {} --username {} --table {}' + \
        " --format TEXT --delimiter {} {} --logfile {} --logfile-log-level INFO " + \
        '--bad-row-file {} {}'
        Yb_load_call = Yb_load_call.format('\"' + path.join(ybtools_path, ybload) + '\"',
                                                 database, 
                                                 userid, 
                                                 table_name, 
                                                 '\"' + delim  + '\"',
                                                 '\"' + file_path + '\"',
                                                 '\"' + LOG_FILE + '\"',
                                                 '\"' + BAD_FILE + '\"',
                                                 yb_params)

        if verbose:
            print("Bulk loading has commenced\n")
        environ['YBPASSWORD'] = passwd
        #making the call
        CLI_CALL = check_call(Yb_load_call, shell = True)
        
        if CLI_CALL != 0:
            del environ['YBPASSWORD']
            print('Error: Something went wront with the bulk load: Here is what was passed to the call')
            print('\n\n ****************************************** \n')
            print(Yb_load_call)
            print('\n\n ****************************************** \n')
            print("There is an error file here: %s \nThere is a log file here %s" % (BAD_FILE,LOG_FILE))
            return(None)
            
    except Exception as e: 
        del environ['YBPASSWORD']
        print(e)
        print('\n\n ****************************************** \n')
        print("There is an error file here: %s \nThere is a log file here %s" % (BAD_FILE,LOG_FILE))
        return(None)
    try:
        remove(LOG_FILE)
        remove(BAD_FILE)
    except:
        pass
    
    if delete_file:
        if verbose:
            print("removing pandas saved file")
        remove(file_path)
           
    return(True)
