import json, requests, os, hashlib, sys, time, pickle, getopt, math, ConfigParser



def checkAuth():
    r = requests.get('http://api.appnexus.com/user')
    resp = json.loads(r.content)
    if resp['response'].get('status', False) != "OK":
            #print "Auth is good"
            return False
    else:
            #print "No auth"
            return True

def saveCookies (cookieFile, cookieJar):
    if os.path.exists(cookieFile):
            os.remove(cookieFile)
            
    f = open(cookieFile, 'wb')
    pickle.dump(cookieJar, f)

def getSavedCookies (cookieFile):
    if os.path.exists(cookieFile):
            f = open(cookieFile, 'rb')
            cookieJar = pickle.load(f)
            #print "Cookies loaded"
            return cookieJar
    else:
            return False

def getAuth(username, password, cookieFile):
    cookieJar = getSavedCookies(cookieFile)
    authUrl = 'https://api.appnexus.com/auth'
    authPayload = {
            "auth":{
                "username":username,
                "password":password
            }
        }
    
    if not cookieJar or not checkAuth():
            r = requests.post(authUrl, data=json.dumps(authPayload))
            resp = json.loads(r.content)
            if resp['response'].get('status', False) != "OK":
                print "Auth failed: " + str(resp['response'])
                return False
            else:
                #print "Successfully authenticated"
                cookieJar = r.cookies
                saveCookies(cookieFile, cookieJar)
    return cookieJar


def getAvailableLogs(cookieJar):
    logListUrl = 'http://api.appnexus.com/siphon'
    r = requests.get(logListUrl, cookies=cookieJar)
    resp = json.loads(r.content)["response"]

    if resp.get("status", False) != "OK":
            return False
    else:
            return resp["siphons"]

def ensureDirExists (path):
    if os.path.isdir(path):
        return True
    elif os.path.exists(path):
        print "Error: path ("+path+") exists but is not directory"
        return False
    else:
        os.makedirs(path)
        if os.path.isdir(path):
            return True
        else:
            print "Tried to create dir ("+path+") but didn't seem to work"
            return False


def isNewLogFile (filename, serverFileMD5):
    if os.path.exists(filename):
            chksumDisk = checksum(filename)
            if serverFileMD5 == chksumDisk:
              return False
            else:
              print filename + " exists, but checksum wrong " + chksumDisk + " / " + serverFileMD5
              return True
    else:
            return True

def buildFileName (dataDir, logType, logHour, timestamp, part, dupe, extension):
    name = dataDir + "/" + logType + "/" + logHour 
    if dupe:
        name += "-dupe-" + timestamp
    name += "_pt" + part + "." + extension
    return name

def downloadFile(url, params, localFile, cookieJar):
    #
    # Setup progress bar
    #
    maxProgress = 40
    sys.stdout.write("\t")
    sys.stdout.write("[%s]" % (" " * maxProgress))
    sys.stdout.flush()
    sys.stdout.write("\b" * (maxProgress+1)) # return to start of line, after '['
    currProgress = 0
    
    #
    # Do the download
    #
    r = requests.get(url, cookies=cookieJar, params=params, stream=True)
    dlData = {}
    dlData["size"] = int(r.headers['content-length'].strip())
    dlData["dlsize"] = 0
    with open(localFile, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
              if chunk: # filter out keep-alive new chunks
                  dlData["dlsize"] += len(chunk)
                  f.write(chunk)
                  f.flush()
                  # update progress bar
                  if math.floor(float(dlData["dlsize"]) / dlData["size"] * maxProgress ) > currProgress:
                    currProgress += 1
                    sys.stdout.write("|")
                    sys.stdout.flush()
    sys.stdout.write("\n")
    return dlData

def checksum(filepath):
    md5 = hashlib.md5()
    blocksize = 8192
    f = open(filepath, 'rb')
    while True:
            data = f.read(blocksize)
            if not data:
              break
            md5.update(data)
    return md5.hexdigest()

     
def checkDupes (logFiles):
    d = dict()
    for log in logFiles:
        k = log["name"] + "-" + log["hour"]
        v = log
        if k in d: # dupe!
            old = d[k]
            oldTimeStamp = old["timestamp"]
            logTimeStamp = log["timestamp"]
            print "Found duplicate for log: " + k
            print "Will keep the one with the newest timestamp ("+oldTimeStamp+" vs "+logTimeStamp+")."
            if logTimeStamp < oldTimeStamp:
                log["dupe"] = True
                k = k + "-" + logTimeStamp
                d[k] = v
            else:
                d[k] = v
                old["dupe"] = True
                k = k + "-" + oldTimeStamp
                d[k] = old            
        else:
            d[k] = v
    return d.values()

def downloadNewLogs (logFiles, dataDir, filter, url_logDownload, cookieJar, minTimePerRequestInSecs):
    maxRetries = 5
    numExisting = 0
    numDownloaded = 0
    numFailed = 0
    numFiltered = 0
    for log in logFiles:
            logType = log["name"]
            ensureDirExists(dataDir + "/" + logType)
            logHour = log["hour"]
            timestamp = log["timestamp"]
            dupe = False
            if "dupe" in log and log["dupe"]:
                dupe = True
            for logFile in log["splits"]:
              splitPart = logFile["part"]
              anChecksum = logFile["checksum"]
              status = logFile["status"] # e.g. new
              filename = buildFileName(dataDir, logType, logHour, timestamp, splitPart, dupe, "gz")
              if filter != '' and filename.find(filter) == -1:
                numFiltered += 1
                continue # skip downloading this one
              if isNewLogFile(filename, anChecksum):
                  #download
                  params_logDownload = dict(
                      split_part=splitPart,
                      hour=logHour,
                      timestamp=timestamp,
                      siphon_name=logType
                  )
                  trys = 0
                  downloadCorrect = False
                  while trys < maxRetries and not downloadCorrect:

                      print "Getting: " + filename + " (try " + str(trys) + ")"
                      timeStart = time.time()
                      dlData = downloadFile(url_logDownload, params_logDownload, filename, cookieJar)
                      timeEnd = time.time()
                      timeElapsed = timeEnd - timeStart
                      dlSpeedk = round(float(dlData["dlsize"])/1024/timeElapsed, 2)
                      dlActual = round(float(dlData["dlsize"])/1024/1024, 2)
                      dlExpected = round(float(dlData["size"])/1024/1024, 2)
                      print "\t" + str(dlActual) + " of " + str(dlExpected) + " MB in " + str(round(timeElapsed, 1)) + " seconds ("+str(dlSpeedk)+" kbps)"
                      trys += 1

                      downloadChecksum = checksum(filename)

                      if downloadChecksum == anChecksum:
                          downloadCorrect = True
                      else:
                          print "\tAppNexus Checksum ("+anChecksum+") doesn't match downloaded file ("+downloadChecksum+")."
                          
                      sleepTime = minTimePerRequestInSecs - timeElapsed
                      if sleepTime > 0:
                          print "Sleeping for " + str(sleepTime) + " seconds"
                          time.sleep(sleepTime)

                  if downloadCorrect:
                      numDownloaded += 1
                  else:
                      print "Failed to successfully download " + filename + ".  Removing."
                      numFailed += 1
                      os.remove(filename)

                  
              else:
                  #already have this one
                  numExisting += 1

    print "Skipped " + str(numFiltered) + " (filtered) files"
    print "Skipped " + str(numExisting) + " (existing) files"
    print "Downloaded " + str(numDownloaded) + " (new/changed) files"
    print "Failed to download " + str(numFailed) + " files."

def main (argv):
  
    # config vars
    configFile = "pulllogleveldata-config" # name of config file that contains all the following
    username = ""
    password = ""
    memberId = "" # appnexus "Seat" id
    dataDir = "" # where to save log files
    requestsPerMin = 25

    # load config
    configFileAbs = os.path.join(os.path.abspath(os.path.dirname(__file__)), configFile)
    Config = ConfigParser.ConfigParser()
    Config.read(configFileAbs)
    username = Config.get("LoginData", "username")
    password = Config.get("LoginData", "password")
    memberId = Config.get("LoginData", "memberId")
    dataDir = Config.get("Paths", "dataDir")
    requestsPerMin = Config.getint("RateLimiting", "requestsPerMin")

    # we do naive throttling (non-optimal) because this script isn't 
    # aware of your other API usage that may be happening simultaneously.
    minTimePerRequestInSecs = 60/requestsPerMin

    
    
    url_logDownload = 'http://api.appnexus.com/siphon-download?member_id=' + memberId
    cookieFile = './authCookies'
    cookieJar = {}
    logFiles = {}

    usage  = "USAGE:\n"
    usage += "pulllogleveldata.py  # download all files we don't currently have in "+dataDir+"\n"
    usage +=     "\t-f <filter>  # only download files matching filter\n"
    usage +=     "\t-d <datadir> # change download location from default\n"
    usage +=     "\t-h           # help\n"

    # parse args
    try:
        opts, args = getopt.getopt(argv,"d:f:h")
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    
    filter = ''
    
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-f", "--filter"):
            filter = arg
        elif opt in ("-d", "--datadir"):
            dataDir = arg
        else:
            print usage
            sys.exit()
    
    #
    # Do the work
    #
    try:
        print "Use CTRL-C to quit.\n"
        print "Authenticating..."
        cookieJar = getAuth(username, password, cookieFile)
        if cookieJar:
            print "Getting log listing..."
            logFiles = getAvailableLogs(cookieJar)
            if logFiles:
                logFiles = checkDupes(logFiles)
                if ensureDirExists(dataDir):
                    print "Downloading new log files..."
                    downloadNewLogs(logFiles, dataDir, filter, url_logDownload, cookieJar, minTimePerRequestInSecs)
                else:
                    print "Could not create data directory."
            else:
                print "ERROR: Could not get log listing."
        else:
            print "Authentication failed."
    except KeyboardInterrupt:
        print "   ...Okay, quitting."
        sys.exit(1)
    
    
    
if __name__ == "__main__":
    main(sys.argv[1:])



