import json, requests, os, hashlib, sys, time, pickle, getopt, math, ConfigParser

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

# we do naive throttling (non-optimal) because your api rate limit is global (all processes)
minTimePerRequestInSecs = 60/requestsPerMin

url_auth = 'https://api.appnexus.com/auth'
url_logList = 'http://api.appnexus.com/siphon'
url_logDownload = 'http://api.appnexus.com/siphon-download?member_id=' + memberId
cookieFile = './authCookies'
cookieJar = {}
logFiles = {}

def checkAuth():
    r = requests.get('http://api.appnexus.com/user')
    resp = json.loads(r.content)
    if resp['response'].get('status', False) != "OK":
        #print "Auth is good"
        return False
    else:
        #print "No auth"
        return True

def saveCookies ():
    if os.path.exists(cookieFile):
        os.remove(cookieFile)
        
    f = open(cookieFile, 'wb')
    pickle.dump(cookieJar, f)

def loadCookies ():
    global cookieJar
    if os.path.exists(cookieFile):
        f = open(cookieFile, 'rb')
        cookieJar = pickle.load(f)
        #print "Cookies loaded"
        return True
    else:
        return False


def doAuth():
    global cookieJar
    
    if not loadCookies() or not checkAuth():
        payload = {
            "auth":{
                "username":username,
                "password":password
            }
        }
        r = requests.post(url_auth, data=json.dumps(payload))
        resp = json.loads(r.content)
        if resp['response'].get('status', False) != "OK":
            print "Auth failed: " + str(resp['response'])
            return False
        else:
            #print "Successfully authenticated"
            cookieJar = r.cookies
            saveCookies()
            return True


def getAvailableLogs():
    global cookieJar, logFiles
    r = requests.get(url_logList, cookies=cookieJar)
    resp = json.loads(r.content)["response"]
    if resp.get("status", False) != "OK":
        return False
    else:
        logFiles = resp["siphons"]
        return True

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
            #print filename + " checksum matches " + serverFileMD5
            return False
        else:
            print filename + " exists, but checksum wrong " + chksumDisk + " / " + serverFileMD5
            return True
    else:
        #print "File ("+filename+") doesn't exist."
        return True

def buildFileName (logType, logHour, timestamp, part, extension):
    return dataDir + "/" + logType + "/" + logHour + "_pt" + part + "." + extension

def downloadFile(url, params, localFile):
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


def downloadNewLogs (filter, minTimePerRequestInSecs):
    numDownloaded = 0
    numSkipped = 0
    for log in logFiles:
        logType = log["name"]
        ensureDirExists(dataDir + "/" + logType)
        logHour = log["hour"]
        timestamp = log["timestamp"]
        for logFile in log["splits"]:
            splitPart = logFile["part"]
            checksum = logFile["checksum"]
            status = logFile["status"] # e.g. new
            filename = buildFileName(logType, logHour, timestamp, splitPart, "gz")
            if filter != '' and filename.find(filter) == -1:
              numSkipped += 1
              continue # skip downloading this one
            if isNewLogFile(filename, checksum):
                #download
                params_logDownload = dict(
                    split_part=splitPart,
                    hour=logHour,
                    timestamp=timestamp,
                    siphon_name=logType
                )
                print "Getting: " + filename
                timeStart = time.time()
                dlData = downloadFile(url_logDownload, params_logDownload, filename)
                timeEnd = time.time()
                timeElapsed = timeEnd - timeStart
                dlSpeedk = round(float(dlData["dlsize"])/1024/timeElapsed, 2)
                dlActual = round(float(dlData["dlsize"])/1024/1024, 2)
                dlExpected = round(float(dlData["size"])/1024/1024, 2)
                print "\t" + str(dlActual) + " of " + str(dlExpected) + " MB in " + str(round(timeElapsed, 1)) + " seconds ("+str(dlSpeedk)+" kbps)"
                numDownloaded += 1
                sleepTime = minTimePerRequestInSecs - timeElapsed
                if sleepTime > 0:
                    print "Sleeping for " + str(sleepTime) + " seconds"
                    time.sleep(sleepTime)
            else:
                #skip
                numSkipped += 1

    print "Downloaded " + str(numDownloaded) + " files"
    print "Skipped " + str(numSkipped) + " files"

def main (argv):
  global dataDir
  
  usage  = "USAGE:\n"
  usage +=  "pulllogleveldata.py  # download all files we don't currently have in "+dataDir+"\n"
  usage +=        "\t-f <filter>  # only download files matching filter\n"
  usage +=        "\t-d <datadir> # change download location from default\n"
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
    if doAuth():
        print "Getting log listing..."
        if getAvailableLogs():
            if ensureDirExists(dataDir):
                print "Downloading new log files..."
                downloadNewLogs(filter, minTimePerRequestInSecs)
                #for log in logFiles:
                #    print str(log["hour"]) + " | " + str(log["name"]) + " | " + str(log["timestamp"] + " | " + log["splits"][0]["part"]

            else:
                print "Could not make data dir."
        else:
            print "ERROR: Could not get log listing."
    else:
        print "Authentication failed."
  except KeyboardInterrupt:
    print "   ...Okay, quitting."
    sys.exit(1)
    
    
    
if __name__ == "__main__":
  main(sys.argv[1:])



