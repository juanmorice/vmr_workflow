# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 06:39:49 2019
Version 1.20 Release
@author: Mannar Maniraj

Modified on Fri Feb 28 10:30:00 2020
Fixed the Linux path -- to /home/<username>/
Python Module to safely handle passwords. 

Modified on Wed Sep 29 15:11:00 2021
Fixed the Windows path -- to //stphome/home/

Assumptions and requirement: Connect to VPN or with in Catalina network. "X:\" drive (path: \\stphome\home\<UserID>)  is mapped for Windows operating system 
and "/home/<UserId>" for Linux or unix.
"x:\"  is a default drive mapped by for all users. By default only the user has access to this drive.

#To retrive your password for use in your code:
from safe_password import readpw
pwd = readpw("py1usta1")

"""
#Parameters
dbname = "pn1uspa1"
pwd = "password"


#main program
import sys
from os import path
import getpass

def createpwdf(dbname="d", pwd="p"):
    if sys.platform.startswith('linux') and path.isdir('/home/'+getpass.getuser()+'/'):
        pwd_path = '/home/'+getpass.getuser()+'/'
    elif sys.platform in ['win32','cygwin'] and path.isdir('//stphome/home/'+getpass.getuser().lower()+'/'):
        pwd_path = '//stphome/home/'+getpass.getuser().lower()+'/'
    else:   
        print("Could not find default location on the system: //stphome/home/"+getpass.getuser().lower()+" or /home/"+getpass.getuser()+"/")
        return ""
    f = open( pwd_path + dbname + ".pwd", "w")
    f.write(pwd)
    f.close()
    return print("Password file for database "+ dbname + " created.")

import os
from dotenv import load_dotenv

# Load .env file from project root (searches up directory tree)
load_dotenv()

if __name__ != "__main__": 
#    createpwdf(dbname, pwd)
#else:
    def readpw(dbname="database name"):
        # FALLBACK 1: Check environment variable first (e.g., YELLOWBRICK_PASSWORD)
        env_var_name = f"{dbname.upper().replace(' ', '_')}_PASSWORD"
        env_pwd = os.getenv(env_var_name)
        if env_pwd:
            return env_pwd
        
        # FALLBACK 2: Original network path logic
        if sys.platform.startswith('linux') and path.isdir('/home/'+getpass.getuser()+'/'):
            pwd_path = '/home/'+getpass.getuser()+'/'
        elif sys.platform in ['win32','cygwin'] and path.isdir('//stphome/home/'+getpass.getuser().lower()+'/'):
            pwd_path = '//stphome/home/'+getpass.getuser().lower()+'/'
        else:   
            print("Could not find default location on the system: //stphome/home/"+getpass.getuser().lower()+"/ or /home/"+getpass.getuser()+"/")
            print(f"TIP: Set environment variable {env_var_name} as fallback.")
            return ""
        try:
            f = open( pwd_path + dbname + ".pwd", "r")
            pwd = f.read()
            f.close()
            return pwd
        except FileNotFoundError:
            print("Password for the database name: " + dbname + " not avaliable. " + pwd_path +" \nUse createpwdf(dbname) function to create the password file. Check spelling of  dbname, linux environment is case sensitive.")
            print("Do you want to create a new password file: Y/N:")
            if input(prompt='Enter "Y" or "N" :').lower() == 'y':
                pwd = getpass.getpass(prompt='Enter the database password :')
                f = open( pwd_path + dbname + ".pwd", "w")
                f.write(pwd)
                f.close()
                return pwd
            else: 
                print ("Password for the database name: " + dbname + " could not be retrieved")
                return ""
        except:
            print("Password could not be retrieved, Error:" , sys.exc_info()[0])
            return ""