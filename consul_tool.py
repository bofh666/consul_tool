#!/usr/bin/env python

import sys, getopt, validators, requests, json, os, base64

def usage():
    "Prints usage message"
    print('Consul import/export tool v0.1 -- imports/exports and decodes/encodes values from/to Consul KV storage to/from local directories')
    print()
    print('Usage:')
    print(sys.argv[0],"m|--mode <import|export> -u|--url=<http://some_consul:8500> -d|--directory=</some/dir> -p|--prefix=</some/prefix>")
    print()
    print('-m, --mode=\t set either "import" or "export" mode')
    print('-u, --url=\t Consul address in form of "scheme://host:port"')
    print('-d, --directory= absolute or relative path to local directory where to save/get values from;')
    print('\t\t in import mode it is used as base directory where subdirectory named after prefix is created;')
    print('\t\t in export mode it is used as source directory, so user can set custom prefix to upload keys/values')
    print('-p, --prefix=\t prefix in Consul KV storage')
    return

def preflight(argv):
    "Parses command line parameters and arguments and performs some basic checks"
    mode = ''
    url = ''
    directory = ''
    prefix = ''
    try:
        opts, args = getopt.getopt(argv, "m:u:d:p:", ["mode=","url=","directory=","prefix="])
    except:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-m", "--mode"):
            if arg not in ("import", "export"):
                print('Mode should be either import or export')
                sys.exit(1)
            mode = arg
        elif opt in ("-u", "--url"):
            if not validators.url(arg):
                print('Consul address is invalid')
                sys.exit(1)
            url = str(arg + "/v1/txn?pretty")
        elif opt in ("-d", "--directory"):
            if not os.path.exists(arg):
                print('Directory', arg, 'doesn\'t exist')
                sys.exit(1)
            directory = arg
        elif opt in ("-p", "--prefix"):
            prefix = arg
    if mode == "" or url == "" or directory == "" or prefix == "":
        usage()
        sys.exit(2)
    return mode, url, directory, prefix

def consul_import(url, directory, prefix):
    "Downloads keys/values from Consul starting at prefix, creates keys directories, decodes values from base64 and puts them into keys files"
    request = [{'KV': {'Verb': 'get-tree', 'Key': prefix}}]
    response = requests.put(url,headers=headers,data=json.dumps(request))
    print(response.text)
    kv = response.json()
    for item in kv['Results']:
        dest = str(directory + "/" + os.path.dirname(item['KV']['Key']))
        if not os.path.exists(dest):
            os.makedirs(dest)            
    for item in kv['Results']:
        key = str(directory + "/" + item['KV']['Key'])
        value = item['KV']['Value']
        if value != None:
            f = open(key, "w+")
            f.write(base64.b64decode(value).decode())
            f.close()
    return

def consul_export(url, directory, prefix):
    "Reads values from files in all subdirectories of directory, encodes them and uploads to Consul KV storage, creating relevant keys"
    source = directory
    request = []
    for root, dirs, files in os.walk(source):
        for name in files:
            kv = {}
            key_file = os.path.join(root, name)
            key = str(prefix + os.path.join(root, name)[len(source):])
            f = open(key_file, "rb")
            value = f.read()
            f.close()
            kv['KV'] = {'Verb': 'set', 'Key': key, 'Value': base64.b64encode(value).decode()}
            request.append(kv)
            response = requests.put(url,headers=headers,data=json.dumps(request))
            print(response.text)
    return

if __name__ == "__main__":
    headers = {'Content-Type': 'application/json'}
    mode, url, directory, prefix = preflight(sys.argv[1:])
    if mode == "import":
        consul_import(url, directory, prefix)
    elif mode == "export":
        consul_export(url, directory, prefix)
