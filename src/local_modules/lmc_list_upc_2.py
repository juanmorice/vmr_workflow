
def LoadJSONfromHTTP(url,retry=3):
    import requests 
    import time
    
    result=None
    retry=int(retry)
    if retry < 1: retry=1
    trycount=0
    while trycount<retry:
        trycount=trycount+1
        try:
            r = requests.get(url)
            j_data = r.json()
            if r.status_code < 200 or r.status_code > 300:
                print(f'*** Connection Unsuccessful. HTTP error code {r.status_code} from: {url}', flush=True)
                time.sleep(1)
                continue
            result = j_data
            break
        except requests.exceptions.RequestException as e:
            print(f"*** Request error fetching JSON from: {url} - {e}", flush=True)
            time.sleep(1)
            continue
        
    return result
 
def LMC_GetSheetUPCs(baseURL, ListID, SheetID):
    BLOCKSIZE=1000
    result=[]
    rows=BLOCKSIZE    
    offset=0
    cursormark="*"
    while(True):
        funcURL = baseURL+"/lists/"+ListID+"/group/"+SheetID+"/select/json/?rows=10000&sort=id+asc&cursorMark="+cursormark
    #    print(funcURL)
        json=LoadJSONfromHTTP(funcURL)
        if len(json["response"]["docs"]) > 0:
            result.extend(json["response"]["docs"])
            cursormark=json["nextCursorMark"]
            continue
        return result

def LMC_PandasGetAllUPCs(baseURL, ListID):
    import pandas as pd    
    AllUPCs=[]
    funcURL = baseURL+"/lists/"+ListID
    listdetails=LoadJSONfromHTTP(funcURL)
    sheets=listdetails["sheets"]
    for sheet in sheets:
        result=LMC_GetSheetUPCs(baseURL, ListID, sheet["uuid"])
        for item in result:
            item.update({"LMC_UPC_List":ListID})
            item.update( {"group_number":sheet["index"]})
            item.update( {"group_name":sheet["name"]})
            item.update({"LMC_LIST_NAME":listdetails["name"]})
        AllUPCs.extend(result)
    df=pd.DataFrame.from_records(AllUPCs,index="id")
    return df
