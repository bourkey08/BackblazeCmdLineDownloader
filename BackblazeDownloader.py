'''
    BackBlaze Backup Downloader, Downloads all restore zips as they become available and optionally extracts them.
    Copyright (C) 2022  Mitchell Bourke

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
'''
import requests, json, hashlib, os, zipfile, struct, time, shutil, sys, re, string, threading, queue, logging, codecs
import multiprocessing.pool
from multiprocessing.pool import ThreadPool
from lxml import etree

def BinaryFormat(size, suffix=''):
    size = int(size)

    Units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']

    i=0
    while size > 1024:
        i += 1
        size = size / 1024.0

    return '{:,.2f}'.format(size) + ' ' + Units[i] + suffix

#Takes a value in the format 10mbps, 25MBps ect and returns the value in bytes
def ParseBinaryUnits(inputval):
    #Define formula for calculating the output value form the input value for a given unit
    Units = {}
    Units['Bps'] = lambda a:int(a)
    Units['KBps'] = lambda a:int(a)*2**10
    Units['MBps'] = lambda a:int(a)*2**20
    Units['GBps'] = lambda a:int(a)*2**30
    Units['TBps'] = lambda a:int(a)*2**40
    Units['PBps'] = lambda a:int(a)*2**50
    Units['EBps'] = lambda a:int(a)*2**60
    Units['bps'] = lambda a:int(a)/8
    Units['kbps'] = lambda a:(int(a)*2**10) / 8
    Units['mbps'] = lambda a:(int(a)*2**20) / 8
    Units['gbps'] = lambda a:(int(a)*2**30) / 8
    Units['tbps'] = lambda a:(int(a)*2**40) / 8
    Units['pbps'] = lambda a:(int(a)*2**50) / 8
    Units['ebps'] = lambda a:(int(a)*2**60) / 8

    value = ''
    unit = ''

    for a in inputval:
        if a in '0123456789':
            value += a
        else:
            unit += a

    if unit not in Units:
        return None

    else:
        return Units[unit](value)

class BackBlazeSession():
    def __init__(self, Email, Password, DiskIOEvent, TokenBucketInst, DiskBuffer, WorkingDirectory='temp', OutputDir='data', threads=32):
        self.__sess = requests.session()
        self.__Email = Email
        self.__Password = Password
        self.__SessionData = None
        self._WorkingDirectory = WorkingDirectory
        self._OutputDir = OutputDir
        self._threads = threads
        self._DiskIOEvent = DiskIOEvent
        self._TokenBucket = TokenBucketInst
        self._DiskBuffer = DiskBuffer

        #Now create the thread pool and the process pool
        self._tpool = ThreadPool(threads)

        #And an object to store the list of restores so that we dont repeatedly start downloading the same file
        self.__Restores = {}

        #Make sure the required directorys exist
        if not os.path.exists(self._WorkingDirectory):
            os.makedirs(self._WorkingDirectory)

        if not os.path.exists(self._OutputDir):
            os.makedirs(self._OutputDir)

        if not self.__Login():
            print ('Error, Login Failed')

    #Implement the hash function that backblaze uses as a sort of checksum for requests
    def BZSanity(self, text):
        hashedstring = hashlib.sha1(codecs.encode(text.encode(), 'hex').lower()).hexdigest().lower()

        return hashedstring[1] + hashedstring[3] + hashedstring[5] + hashedstring[7]

    #Handles logging in and getting the token we need to list/download restores
    def __Login(self):        
        resp = self.__sess.post('https://api.backblazeb2.com/b2api/v1/b2_create_session', json={
            "identity": { "identityType":"accountEmail", "email": self.__Email},
            "clientInfo": { "deviceName":"bzclient", "clientType":"com/backblaze/backup/win"}
        })

        if resp.status_code == 200:
            data = json.loads(resp.content)

            resp = self.__sess.post(data['apiUrl'] + '/b2api/v1/b2_present_credentials', json={            
              "credentials": { "credentialsType":"password", "password": self.__Password
                         },
              "infoRequested": ["accountProfile"],
              "authToken": data['authToken']
            })

            #Store the session data as its needed for making future requests
            self.__SessionData = json.loads(resp.content)

            return True

        else:
            return False

    #Class to provide a wrapper for a single restore
    class __Restore():
        def __init__(self, parent, restore, EMail, authToken):
            self.__restore = restore
            self.__parent = parent
            self.__Email = Email
            self.__authToken = authToken

        def __getattr__(self, item):
            if item in self.__restore:
                return self.__restore[item]
            else:
                raise AttributeError

        def __getitem__(self, item):
            if item in self.__restore:
                return self.__restore[item]
            else:
                return AttributeError

        def iterkeys(self):
            return iter(self.__restore.keys())

        def keys(self):
            return list(self.__restore.keys())

        def __iter__(self):
            for key in self.__restore.keys():
                yield key, self.__restore[key]

        def __repr__(self):
                if 'display_filename' in self.__restore:
                    return self.__restore['display_filename']
                else:
                    return self.__restore['filename']

        #Returns the headers to be used for authorisation when downloading this restore file
        def RestoreHeaders(self):
            return {
                'Authorization': self.__authToken,
                'User-Agent': 'backblaze_agent/8.0.1.567'
            }

        #Returns the url and post data for fetching a specific range of bytes from this restore
        def RestoreRangeReq(self, Start, End):           
            url = 'https://' + self.__restore['serverhost'] + '.backblaze.com/api/restorezipdownloadex'

            postdata = {
                'version': '8.0.1.567',
                'hexemailaddr': codecs.encode(self.__Email.encode(), 'hex'),
                'request_firstbyteindex': Start,
                'request_numbytes': End,
                'hexpassword': codecs.encode(b'Null', 'hex'),
                'hguid': self.__restore['hguid'],
                'rid': self.__restore['rid'],
                'bz_v5_auth_token': self.__restore['bz_auth_token'],
                'bzsanity': self.__parent.BZSanity(self.__Email)
            }

            return (url, postdata)

        def __DownloadPiece(self, Start, End, _TokenBucket):
            try:       
                #As we are requesting 16MB of data set stream to true so that we can start downloading as soon as the first section is ready rather than waiting for the whole chunk to buffer
                #The timeout is set to 30 seconds to allow for breif slowdowns while still aborting transfers that are stuck so that they may be added back into the queue to be retryed
                resp = requests.post(self.RestoreRangeReq(Start, End-Start)[0], data=self.RestoreRangeReq(Start, End-Start)[1], headers=self.RestoreHeaders(), stream=True, timeout=30)
                
                if resp.status_code != 200:
                    return (False, Start, End, None)
                else:
                    data = []

                    #Iterate over the response in smaller peices and store in a list that we are using as a buffer
                    #Smaller block sizes will increase cpu usage but give more consistent download speeds than downloading in 16MB chunks due to handling network congestion better 
                    #65k seems to be a good trade off on a 250mbps connection                    
                    _TokenBucket.RequestBytes(0xffff)
                    for a in resp.iter_content(0xffff):
                        data.append(a)

                        #Now request permission to read another 65k
                        _TokenBucket.RequestBytes(0xffff)

                    data = b''.join(data)

                    #Handle the case where the returned data is not the correct length
                    if len(data[24:-56]) != End-Start:                        
                        #print ('Data Length Does Not Match {0} {1} {2} {3}'.format(len(resp.content[24:-56]), End-Start, Start, End))
                        return (False, Start, End, None)

                    return (True, Start, End, data[24:-56])
            except:
                return (False, Start, End, None)

        #Starts downloading the restore
        def Download(self, ExtractWhenDone=False):
            print ('Downloading: ' + self.__restore['display_filename'])
            print ('Total Size: ' + BinaryFormat(self.__restore['zipsize']))

            #Used to track the last time the console was updated so we can only draw updates once per second
            LastConsoleUpdate = 0

            #Build a list and fill it out with chunks that need to be downloaded based on the file size (using 16MB chunks)
            #We are using a list instead of a set as we want to start downloading from the start of the file to the end
            ChunksToDownload = []

            #Track how many times we have retried to fetch a given chunk, this allows us to stop rather than continuing forever on an unresolvable error
            ChunkRetryCount = {}

            #Also define a a variable so we can track our progress
            TotalChunks = 0
            ToDownloadChunks = 0
            DownloadedChunks=0
            StartTime = time.time()

            pos = 0
            while pos < int(self.__restore['zipsize']):
                ChunksToDownload.append((pos, min(int(self.__restore['zipsize']), pos+0xffffff)))
                pos += 0xffffff

                #Tally up the total number of chunks
                TotalChunks += 1

            #Copy the total into chunks to download and then we will subtract from this when removing already downloaded data
            ToDownloadChunks = TotalChunks

            #First check if there is log for this restore and if there is load the list of chunks that have already been downloaded
            if os.path.exists(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename'] + '.db')):
                with open(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename'] + '.db'), 'rb') as fil:
                    entry = fil.read(16)

                    while entry:
                        val = struct.unpack('QQ', entry)

                        #Remove the block from the list of blocks to download as we have already downloaded it
                        if val in ChunksToDownload:
                            ChunksToDownload.remove(val)
                            ToDownloadChunks -= 1

                        #And get the next entry
                        entry = fil.read(16)
            
            #And now sort chunks to download into reverse order by the start position, we want them in reverse order as well iterate over the list backwards so we can remove elements inplace
            ChunksToDownload.sort(key=lambda a:a[0], reverse=True)            
            
            #Define a list for keeping track of in progress downloads format is [(thread, starttime),]
            results = []               

            #Now lets start downloading missing pieces of the file until we have all pieces
            while len(ChunksToDownload) > 0 or len(results) > 0:    
                #Iterate over the list of entrys in results list and retreive the output of any finished jobs then remove them from the list
                for x in range(len(results) - 1, -1, -1):          
                    #If this job has finished executing
                    if results[x][0].ready():
                        result = results[x][0].get()

                        #If fetching the chunk failed, add it back into the list of missing chunks
                        if result[0] != True:
                            if (result[1], result[2]) not in ChunkRetryCount:                                                                       
                                ChunksToDownload.append((result[1], result[2]))
                                ChunkRetryCount[(result[1], result[2])] = 1 
                                
                            elif ChunkRetryCount[(result[1], result[2])] < 10:
                                ChunksToDownload.append((result[1], result[2]))
                                ChunkRetryCount[(result[1], result[2])] += 1

                            else:
                                ChunksToDownload.append((result[1], result[2]))
                                print ('Failed to download chunk: {0}-{1}'.format(result[1], result[2]))
                                print ('')

                        else:#Otherwise insert the returned data at the correct location in the memory mapped file                                                             
                            #Add the downloaded chunk to the output buffer
                            self.__parent._DiskBuffer.write(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename']), (result[1], result[2]), result[3])                              
                           
                            #Mark the disk buffer as requiring flushing, but only after all other results have been written
                            AnyResultsFound = True

                            #And increment downloaded chunks by 1
                            DownloadedChunks += 1     
                            ToDownloadChunks -= 1

                        #And remove the entry from the results list
                        del results[x]                    

                #Make sure there are enough jobs queueed up to keep the thread pool busy, note ram usage when waiting on disk io will be this value * 16MB + Disk Cache * 16MB
                while len(results) < self.__parent._threads +1 and len(ChunksToDownload) > 0:
                    #Submit tasks to the threadpool to download all missing chunks, max concurrency is limited by pool size
                    for x in range(len(ChunksToDownload) -1, -1, -1):                             
                        results.append([self.__parent._tpool.apply_async(self.__DownloadPiece, (ChunksToDownload[x][0], ChunksToDownload[x][1],self.__parent._TokenBucket)), time.time()])

                        del ChunksToDownload[x]

                        #If the target backlog size has been reached, stop adding new tasks
                        if len(results) >= self.__parent._threads + 1:
                            break
                
                #Finally update the current status
                if LastConsoleUpdate < time.time() -1:
                    print (' ' * 100, end='\r')#Clear previous line
                    print ('Downloading: {0}%  {1}'.format(                        
                        '{:.2f}'.format((1 - (ToDownloadChunks / float(TotalChunks))) * 100),
                        BinaryFormat((DownloadedChunks*0xffffff) / (time.time() - StartTime), 'ps')                        
                    ), end='\r')

                    LastConsoleUpdate = time.time()

                #Wait 0.1 second before checking progress again
                time.sleep(0.1)

            #Flush the file to disk before returning, this ensures all file handles are closed 
            self.__parent._DiskBuffer.flush(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename']))

            return os.path.join(WorkingDirectory, self.__restore['display_filename'])

    #Returns a list of restores, where each restore is an instance of the __Restore Object
    @property
    def Restores(self):
        if self.__SessionData == None:
            if not self.__Login():
                raise Exception('Unable To Login')

        resp = self.__sess.post('https://ca001.backblaze.com/api/restoreinfo', data={
            'version': '8.0.1.567',
            'hexemailaddr': codecs.encode(self.__Email.encode(), 'hex'),
            'hexpassword': codecs.encode(b'Null', 'hex')
        }, headers={
            'Authorization': self.__SessionData['authToken'],
            'User-Agent': 'backblaze_agent/8.0.1.567'
        })

        #If this was successfull then iterate over the returned restores and yield each restore
        if resp.status_code == 200:
            tree = etree.fromstring(resp.content)

            for restore in tree.iter('restore'):
                RestoreEntry = {}

                for key in list(restore.keys()):
                    RestoreEntry[key] = restore.get(key)

                if RestoreEntry['rid'] not in self.__Restores:     
                    self.__Restores['rid'] = self.__Restore(self, RestoreEntry, self.__Email, self.__SessionData['authToken'])
                
                yield self.__Restores['rid']

#Implement a worker that will handle the extraction of zip files after they have been downloaded(if enabled).
#This needs to run in its own thread so that downloading the next zip file can begin while the previous one is being extracted
class ZipExtrator(threading.Thread):
    #When the zip extractor is first initilized a job queue will be passed in as well as an event object, this is to allow the downloader to signal that it needs the zip extractor to pause disk operations
    def __init__(self, JobQueue, IOEvent, ZipRegex, ZipOutPath, KeepZips):
        threading.Thread.__init__(self)
        self.__JobQueue = JobQueue
        self.__IOEvent = IOEvent
        self.__ZipRegex = ZipRegex
        self.__ZipOutPath = ZipOutPath
        self.__KeepZips = KeepZips

        #Automatically start the thread when its first created
        self.start()

    #Main event loop
    def run(self):
        while True:
            #Get the next job from the job queue, we will wait indefinatly for the next job to be available as it could be hrs between zips
            srcfilename = self.__JobQueue.get()

            #Open the zipfile for reading
            zipfil = zipfile.ZipFile(srcfilename, 'r')
            
            #Now iterate over the files to be extracted and for each file calculate the output path, first attempting to match regular expressions in order and then falling back to the default
            for _file in zipfil.filelist:
                filename = _file.filename

                #Set the default outpath to null, if this is still null after trying to match all regular expressions we will fallback to the default path
                outpath = None
                for folder in self.__ZipRegex:                    
                    #Attempt to match regex but only if there is a filter ('', 'path') functions the same as ('.*', '')
                    match = None
                    if folder[0] != '':
                        match = re.match(folder[0], filename)
                        
                    if folder[0] == '' or match != None:
                        #Calculate save path, the file path matches a regular expression, use the path from the regular expression, otherwise use the default path
                        if match != None:
                            outpath = os.path.join(folder[1], filename[match.end():])

                #No regular expressions matched, extract to the default path
                if outpath == None:
                    outpath = os.path.join(self.__ZipOutPath, filename)
                                                   
                #Now check if the file already exists, if it does skip it but only if the files are the exact same size
                self.__IOEvent.wait()#When extracting lots of small files checking if it exists and making directorys accounts for as much iops as actually extracting the file so wait for io event here as well
                if os.path.exists(outpath) and os.path.getsize(outpath) == _file.file_size:
                    pass

                else:
                    #Make sure the directory exists
                    if not os.path.exists(os.path.split(outpath)[0]):
                        os.makedirs(os.path.split(outpath)[0])

                    #Finally extract the file
                    with open(outpath, 'wb') as outfile:
                        with zipfil.open(_file.filename) as infile:
                            self.__IOEvent.wait()#Wait for the event before every io operation, this causes negligible cpu usage compared to the extraction of the zip file
                            block = infile.read(0xffffff)

                            while block:                                        
                                self.__IOEvent.wait()
                                outfile.write(block)

                                self.__IOEvent.wait()
                                block = infile.read(0xffffff)

            #Close the zip file as we are now done with it
            zipfil.close()

            #If the zips are to be kept, move the zipfile to the output directory
            if self.__KeepZips != False:
                shutil.move(srcfilename, os.path.join(self.__ZipOutPath, os.path.split(srcfilename)[1]))

            else:
                #Otherwise remove it now
                try:
                    os.remove(srcfilename)
                except:
                    pass

#Implements a token bucket for throttling the combined download speeds of all threads to the to the specified value, ensuring all threads get a fair share of the download bandwith
#Needs to be thread safe and provide a fair allocation of bandwith to each thread while still allowing 1 thread to use more than "throttle limit" / "Thread Count" if other threads are slow
class TokenBucket():
    def __init__(self, ThrottleLimit, WindowSize=50):#WindowSize is the time interval to throttle over and is in ms
        self.__ThrottleLimit = ThrottleLimit
        if ThrottleLimit != False:
            self.__Locks = [threading.Lock(), threading.Lock()]#First lock is for working with state of token bucket, second lock is for working with queue
            self.__Queue = queue.Queue()
            
            self.__Window = WindowSize / 1000.0
            self.__LastUpdate = time.time()
            self.__BytesAvail = ThrottleLimit * self.__Window

    #Request permission to download a specified number of bytes, optionally if willing to accept a smaller allocation provide the minimum acceptable value to wait for in minimum
    def RequestBytes(self, requested, minimum=None):
        #If there is no throttling, return immediatly
        if self.__ThrottleLimit == False:
            return requested

        #If no minimum was provided then the minimum is the requested ammount
        if minimum == None:
            minimum = requested

        #We always need to get the 2nd lock before attempting to do anything with the pending queue
        self.__Locks[1].acquire()

        #First try to acquire the lock on the token buckets state, we cant do anything without this lock
        if not self.__Locks[0].acquire(False):
            ourevent = threading.Event()

            self.__Queue.put(ourevent)

            #Now release the queue lock
            self.__Locks[1].release()

            #Now wait for our lock to be acquired
            ourevent.wait()

            #And then acquire the main lock as it should now be available
            self.__Locks[0].acquire()

        else:
            #Now release the queue lock
            self.__Locks[1].release()

        #Now that we have an exclusive lock on the main state we need to work out if there is sufficent bytes available to fufill our request immediatly
        #First add the number of bytes that will have become available since the counter was last updated(Clamped to at most windowsize)
        self.__BytesAvail = min(self.__ThrottleLimit * self.__Window, self.__BytesAvail + ((time.time() - self.__LastUpdate) * (self.__ThrottleLimit * self.__Window)))

        #Now check if there are sufficent bytes available to satisfy the full request
        GotBytes = None#Track the number of bytes that we have obtained 
        if self.__BytesAvail >= requested:
            #Set gotbytes to the full requested ammount
            GotBytes = requested

            #And subtract the number of requested bytes from the number of available bytes
            self.__BytesAvail -= requested

        #Otherwise if a minimum was provided for a partial request, check if there are sufficent bytes available to satisfy that
        elif minimum != None and self.__BytesAvail >= minimum:
            #Set got bytes to the number of bytes available (this will be between minimum and requested)
            GotBytes = self.__BytesAvail

            #And as we have taken all available bytes, clear bytes available
            self.__BytesAvail = 0

        else:#Otherwise wait until the minimum number of bytes become available(minimum or requested if no minimum provided)
            #Calculate how long we need to sleep for the correct number of bytes to be available and then wait
            time.sleep((minimum - self.__BytesAvail) / (self.__ThrottleLimit))
            GotBytes = requested if minimum == None else minimum

            #Now clear bytes available as we are using all of them
            self.__BytesAvail = 0

        #And now we have allocated bytes, update the lastupdate time
        self.__LastUpdate = time.time()

        #Finally before returning the number of bytes that were obtained, we need to check if if there are any requests queued up and trigger the next entry to start
        self.__Locks[1].acquire()

        try:
            #Set the event on the first entry in the queue
            self.__Queue.get(0).set()

        except:#if getting an entry from the queue fail then just move on
            pass

        self.__Locks[1].release()
        self.__Locks[0].release()

        #And return the number of bytes obtained
        return GotBytes

#Implement a write buffer for writing downloaded chunks to disk, takes a single argument BufferSize, this is the number of chunks to allow in buffer (each chunk is 16MB)
#This allows chunks that have been downloaded to be placed into a queue to be written out to disk in order to allow the threads to immediatly start downloading the next chunk without waiting for disk io
#The ideal size of the write buffer will depend on Disk IO latency, Internet bandwith and available Ram to use as a cache. 
#On fast flash storage/slow connections this can be set very low to reduce ram usage without impacting download speed.
class WriteBuffer(threading.Thread):
    #Size sets the queue size(16MB per entry), MaxIdle determins how long a file will be kept open without any writes
    def __init__(self, DiskIOEvent, Size=10, MaxIdle=30):
        threading.Thread.__init__(self)

        self.__MaxIdle = MaxIdle
        self.__DiskIOEvent = DiskIOEvent

        #Define a queue that entrys needing to be written to disk will be added to
        self.__WriteBuffer = queue.Queue(Size)

        #And define a dict that we will use to keep track of the number of chunks in the queue for each file, this allows waiting for a file to completly write to disk when flush() is called
        self.__FileChunkCount = {};
        self.__FileChunkLock = threading.Lock()#And we need a lock to ensure 2 threads dont try to modify the dict at the same time

        #Define a dict for storing open file handles, this allows a file to be held open for a period of time rather than being opened and closed for each write
        self.__FileHandles = {}#Format is filename: {'handle': fp, 'dbhandle': fp, 'LastUsed': time.time()}
        self.__FileHandlesCleanupLock = threading.Lock()#Prevents filehandles being removed when a flush operation is waiting to close them explicitly

        #Automaticlaly start the thread when its first created
        self.start()

    def run(self):
        #Keep track of the last time we checked for idle files, we only want to do this once per second rather than after each chunk 
        LastIdleCheck = time.time()

        while True:
            #If the queue is less than 50% full, resume disk extractor IO
            if self.__WriteBuffer.qsize() / float(self.__WriteBuffer.maxsize) < 0.5:
                self.__DiskIOEvent.set()

            #Wait for more data to be added to the queue, use a 1 second delay so that we can periodically wake up even if there no new data to close idle files
            try:
                Entry = self.__WriteBuffer.get(timeout=1)

            except queue.Empty:#If the queue is empty this will raise an Empty error so set entry to None
                Entry = None

            #If the current queue length is >= than 50% of its capacity, pause the zip extractors IO
            if self.__WriteBuffer.qsize() / float(self.__WriteBuffer.maxsize) >= 0.5:
                self.__DiskIOEvent.clear()

            #If we have retreived an entry to write out, do that now
            if Entry != None:
                #Make sure we have the file open and if we dont open it now
                if (Entry[0] in self.__FileHandles) == False:
                    self.__FileHandles[Entry[0]] = {                        
                        'LastUsed': 0
                    }

                    #Make sure the directory for the file exists and create it if its missing
                    if os.path.split(Entry[0])[0] != '':#If the file is being written to a sub directory
                        if not os.path.exists(os.path.split(Entry[0])[0]):
                            os.makedirs(os.path.split(Entry[0])[0])

                    #Make sure the file exists, if it does not create an empty file
                    if not os.path.exists(Entry[0]):
                        with open(Entry[0], 'wb') as fil:
                            pass

                    #Now open the file 
                    self.__FileHandles[Entry[0]]['handle'] = open(Entry[0], 'rb+')

                    #Now open the db file for appending
                    self.__FileHandles[Entry[0]]['dbhandle'] = open(Entry[0] + '.db', 'ab+')

                #Now write the data to the specified position in the file (position is specified as a tuple of (start, end))
                self.__FileHandles[Entry[0]]['handle'].seek(Entry[1][0])
                self.__FileHandles[Entry[0]]['handle'].write(Entry[2])

                #And log that this chunk has been downloaded
                self.__FileHandles[Entry[0]]['dbhandle'].write(struct.pack('QQ', Entry[1][0], Entry[1][1])) 

                #And now flush the files to disk, this prevents data loss if the program is closed when there are still open file handles (although we will use __del__ to attempt to flush buffer on close)
                self.__FileHandles[Entry[0]]['handle'].flush()
                self.__FileHandles[Entry[0]]['dbhandle'].flush()

                #Now decrease the pending chunks for this file by 1
                try:
                    self.__FileChunkLock.acquire()

                    self.__FileChunkCount[Entry[0]] -= 1

                finally:
                    self.__FileChunkLock.release()

                #Finally update the last used time of the file
                self.__FileHandles[Entry[0]]['LastUsed'] = time.time()                    

            #Now if we have not checked for idle files for >= 1 second, do that now
            if time.time() - 1 > LastIdleCheck:
                #Try to aquire the lock required to cleanup file handles, if this fails, skip cleanup operation
                if not self.__FileHandlesCleanupLock.acquire(False):
                    continue         

                for key in list(self.__FileHandles.keys()):
                    if self.__FileHandles[key]['LastUsed'] < time.time() - self.__MaxIdle:
                        #Make sure there are no pending writes for this file in the queue, if there are then keep the file open until those writes complete
                        try:
                            self.__FileChunkLock.acquire()

                            if key in self.__FileChunkCount:
                                if self.__FileChunkCount[key] > 0:
                                    continue#Pending data in the write queue, skip this entry and keep it open

                            #Otherwise lets close the file
                            self.__FileHandles[key]['handle'].close()
                            self.__FileHandles[key]['dbhandle'].close()

                            #Remove the entry from the file handles dict as the file has now been closed
                            del self.__FileHandles[key]

                            #And remove the chunk count for this file (as its 0)
                            if key in self.__FileChunkCount:
                                del self.__FileChunkCount[key]                                

                        finally:
                            self.__FileChunkLock.release()

                #Finally update the last idle check to be now
                LastIdleCheck = time.time()

                #And release the cleanup lock
                self.__FileHandlesCleanupLock.release()

    #Waits for all pending writes to a file to complete and then closes the file handle
    def flush(self, filename):   
        try:
            #Aquire the cleanup lock to ensure that the file handle is not removed by automatic cleanup operations while we are waiting for it to complete
            self.__FileHandlesCleanupLock.acquire()

            #If there is no entry in the filehandles dict for this file then return immediatly
            if filename not in self.__FileHandles:
                return True

            while True:
                #As flush is blocking, pause disk io from the zip extractor until it completes
                self.__DiskIOEvent.clear()

                try:
                    self.__FileChunkLock.acquire()

                    #Is there still pending data to be written?
                    if filename in self.__FileChunkCount:                    
                        if self.__FileChunkCount[filename] > 0:                        
                            continue#Pending data in the write queue, skip this entry and keep it open
                    
                    self.__FileHandles[filename]['handle'].close()
                    self.__FileHandles[filename]['dbhandle'].close()

                    #Remove the entry from the file handles dict as the file has now been closed
                    del self.__FileHandles[filename]

                    #And remove the chunk count for this file (as its 0)
                    if filename in self.__FileChunkCount:
                        del self.__FileChunkCount[filename]

                    #And restart the zip extractor if running
                    self.__DiskIOEvent.set()

                    return True

                finally:
                    self.__FileChunkLock.release()

                #Wait 100ms between checking if the file has been closed
                time.sleep(0.1)
        finally:
            self.__FileHandlesCleanupLock.release()

    #Writes the specified data to the file at the specified position
    def write(self, filename, pos, data):
        #Get an exclusive lock on the file chunk count
        try:
            self.__FileChunkLock.acquire()

            #Increment the pending chunks for this file by 1, creating the key if its not yet present
            if filename not in self.__FileChunkCount:
                self.__FileChunkCount[filename] = 1
            else:
                self.__FileChunkCount[filename] += 1
        finally:
            self.__FileChunkLock.release()     

        #Now add an entry to the queue to write this chunk to disk, this will block if the write buffer is full
        self.__WriteBuffer.put([filename, pos, data])

    #Returns the current writebuffer queue length
    @property
    def QueueLength(self):
        return self.__WriteBuffer.qsize()

    #Called when the write buffer instance is destroyed, flushes any current entrys to disk
    def __del__(self):
        try:
            self.__FileChunkLock.acquire()

            for key in self.__FileChunkCount.keys():
                self.flush(key)

        finally:
            self.__FileChunkLock.release()

#Print help info to the console
def ShowHelp():
    print ('''    
This software is in no way supported by, endorsed by or maintained by Backblaze.

Example Usage: 
    BackblazeDownloader.exe -email=test@example.com -password=mypassword -tempdir=temp -outputdir=data -extract=true -keepzip=true

Additional Options:
    -help - Shows this text
    -licence - Print the licence details to console
    -licence=full - Prints a copy of the Entire GPL V3 Licence

    -threads - Sets how many threads should be run in parallel when downloading backups. Default: 32.
    -zipregex=regularexpression,targetpath - This argument can be specified multiple times and overwrites the path that files will be extracted to for any files that match the provided regular expression
        Example: -zipregex=^c\\/users\\/test\\/.*,d:\\testuser would extract all files with the path c/users/test to d:\testusers, all other files would be put in the folder specified by -outputdir

    -writebuffer=<int> - Sets the size of the write buffer in 16MB chunks (eg a value of 10 will use 160MB of ram for the write buffer) Default: 10

Additional Notes:
    Closing this program during a download will not cause the download to be restarted from the begining.
    Files are downloaded in 16MB chunks and a list of chunks that have been downloaded is saved along with the partially downloaded file.
    This allows the download to resume from the last fully downloaded chunk when relaunched.

    If extracting of zip files after downloading is completed is enabled, the client will extract the zipfile in a background thread and begin the next file download if one is available.
    The ZIP extraction will be automatically throttled to prevent disk performance becomming a bottleneck when the download temp directory and the extraction target is on the same drive. 
''')

#Prints the licence to the console, optionally print the entire GPL v3 licence to the console if details is true
def ShowLicence(details=False):
    print (''' 
    BackBlaze Backup Downloader, Downloads all restore zips as they become available and optionally extracts them.
    Copyright (C) 2022  Mitchell Bourke

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
    ''')

    #If details is true, fetch the full licence from the gnu website and print it to console
    if details:
        resp = requests.get('https://www.gnu.org/licenses/gpl-3.0.txt')

        if resp.status_code == 200:
            print (resp.content)
        else:
            print ('Error Retreiving licence file')


if __name__ == '__main__':
    #Load the list of backups we have already downloaded
    DownloadedBackups = []

    if os.path.exists('Downloaded.db'):
        with open('Downloaded.db', 'r') as fil:
            line = fil.readline()

            while line:
                DownloadedBackups.append(line.rstrip('\r\n').rstrip('\n'))

                line = fil.readline()

    #Set defaults for all fields
    threads = 32
    Email = None
    Password = None
    WorkingDirectory = 'temp'
    OutputDirectory = None
    writebuffersize = 10

    Extract = False
    KeepZip=True
    Throttle = False
    
    #Regular expressions to use when extracting the zip file, usefull for extracting different subfolders form the zipfile to different locations
    ZipRegex = []

    #Now parse any command line arguments, we want to accept arguments in either of 2 formats -arg=value or -arg value
    for x in range(0, len(sys.argv)):
        if '=' in sys.argv[x] and sys.argv[x].startswith('-'):
            field, _, value = sys.argv[x].lstrip('-').partition('=')

        elif x > 1 and sys.argv[x-1].startswith('-'):
            field = sys.argv[x-1].lstrip('-')
            value = sys.argv[x]

        elif sys.argv[x] == '-help':
            field = 'help'

        #The help command will print both 
        elif sys.argv[x] == '-licence':
            field = 'licence'

        else:
            continue

        #Make the field argument case insensitive by forcing it to lower case
        field = field.lower()

        if field == 'help':
            ShowHelp()

        elif field == 'licence':
            ShowLicence(True if value == 'full' else False)

        elif field == 'threads':
            if not value.isdigit():
                print ('Argument for threads must be an integer between 1 and 512')
                exit(1)

            threads = int(value)

            if threads < 1 or threads > 512:
                print ('Argument for threads must be an integer between 1 and 512')
                exit(1)

        elif field == 'email':
            Email = value

        elif field == 'password':
            Password = value

        elif field == 'tempdir':
            WorkingDirectory = value

        elif field == 'outputdir':
            OutputDirectory = value

        elif field == 'extract':
            if value.lower() in ('true', 't', 'y', 'yes'):
                Extract = True
            else:
                Extract = False

        elif field == 'keepzip':
            if value.lower() in ('false', 'f', 'n', 'no'):
                KeepZip = False
            else:
                KeepZip = True

        elif field == 'throttle':
            print ('Throttling is currently not working properly so is disabled')
            continue

            #The value for throttle should be an integer followed by a unit, check that this is the case and that the provided unit is valid
            Throttle = ParseBinaryUnits(value)

            if Throttle == None:
                print ('Throttle must be a valid speed and unit, see -help for details')
                exit(1)

        #Multiple regular expressions can be provided by adding multiple -zipregex arguments
        elif field == 'zipregex':
            ZipRegex.append((value.split(',')[0], value.split(',')[1]))


        #Set the size of the write buffer, this must be an integer between 1 and ((available ram / 16MB) - threadcount)
        elif field == 'writebuffer':
            try:
                writebuffersize = int(value)
            except:
                print ('Invalid value for writebuffer')
                exit(0)

    #If the user has not provided the bare minimum arguments, default to showing help
    if Email == None or Password == None or OutputDirectory == None:
        ShowHelp()
        input('')
        exit(0)

    #Define an event that will be passed to both the downloader and the zipextractor if enabled, this allows the downloader to signal to the zip extractor that it needs the zip extractor to stop IO operations
    #This allows the downloader to temporally pause the zip extractor in the event its being bottlenecked by writing output chunks to disk due to the zip extractor maxing disk iops
    DiskIOEvent = threading.Event()
    DiskIOEvent.set()#When the event is set, the zip extractor will run, clearing the event will cause the zip extractor to pause until its set again

    #If extracting of zip files is enabled create a job queue for the zip extractor and then get an instance of the zip extract and start it
    if Extract:
        #Job queue that we will add the name of zip files to once they are ready to be extracted
        zipjobqueue = queue.Queue()
        
        #And get an instance of the zip extractor and start it in its own thread, this allows the next zip file to begin downloading while the previous one is being extracted
        zipextrator = ZipExtrator(zipjobqueue, DiskIOEvent, ZipRegex, OutputDirectory, KeepZip)

    #Create an instance of the disk buffer object and pass in the DiskIOEvent
    DiskBuffer = WriteBuffer(DiskIOEvent, writebuffersize)

    #If a throtte was specified, get a throttler object, if throttle is false this returns an object that always immediatly returns 
    TokenBucketInst = TokenBucket(Throttle)

    while True:   
        try:     
            BB = BackBlazeSession(Email, Password, DiskIOEvent, TokenBucketInst, DiskBuffer, WorkingDirectory, OutputDirectory, threads)
            
            #Track if we have found a file to download in this iteration, if we have not then we will sleep for 15 minutes before checking again, otherwise check again immediatly
            FileFound = False

            #Build a list of zip file names that are attatched to existing backups
            ZipFiles = []
            for a in BB.Restores:
                ZipFiles.append(a.display_filename)

            #Now iterate over the working directory and find any zip files that are not linked to exising backups and schedule them to be extracted(if enabled, otherwise just move them)
            for filename in os.listdir(WorkingDirectory):
                if filename.rpartition('.')[2].lower() == 'zip':
                    if filename not in ZipFiles:
                        ZipPath = os.path.join(WorkingDirectory, filename)

                        #If extract zips is enabled
                        if Extract:
                            zipjobqueue.put(ZipPath)

                        else:#Otherwise just move the downloaded file to the output location
                            shutil.move(ZipPath, os.path.join(OutputDirectory, os.path.split(ZipPath)[1]))

            #Iterate over restores and search for any that we have not yet downloaded that are not in progress
            for a in BB.Restores:
                if a.rid not in DownloadedBackups:
                    if a.restore_in_progress == 'false':
                        FileFound = True

                        ZipPath = a.Download()
                        
                        #If we are to extract the zip file, then add the zipfile to the zip extractors job queue
                        if Extract:
                            zipjobqueue.put(ZipPath)

                        else:#Otherwise just move the downloaded file to the output location
                            shutil.move(ZipPath, os.path.join(OutputDirectory, os.path.split(ZipPath)[1]))

                        #Update the list of downloaded backups to include this one
                        DownloadedBackups.append(a.rid)

                        #And append a line to the downloaded backups file as well so we dont try again if the program is stopped and started
                        with open('Downloaded.db', 'a') as fil:
                            fil.write(str(a.rid) + '\n')

                else:#If we have already downloaded this backup, check to make sure the zipfile has been moved/extract and if not add it to the queue
                    ZipPath = os.path.join(WorkingDirectory, a.display_filename)
                    
                    if os.path.exists(ZipPath):
                        #If we are to extract the zip file, then add the zipfile to the zip extractors job queue
                        if Extract:
                            zipjobqueue.put(ZipPath)

                        else:#Otherwise just move the downloaded file to the output location
                            shutil.move(ZipPath, os.path.join(OutputDirectory, os.path.split(ZipPath)[1]))

            #Wait 15 minutes before checking again for more files to download but only if we did not find any files to download
            if not FileFound:
                print ('Waiting for more restores to be available')
                time.sleep(900)
        except:
            print ('Error checking for backups to download, waiting 5 minutes before retrying')
            time.sleep(300)
    
