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
from __future__ import print_function

#Supress the warning about cryptography not supporting python 2.7 in the next release, will update code to be python 3 compatable
import warnings
warnings.filterwarnings("ignore")

import requests, json, hashlib, os, zipfile, struct, time, shutil, sys, re, string, threading, Queue, logging
import multiprocessing.pool
from multiprocessing.pool import ThreadPool
from lxml import etree

def BinaryFormat(size, suffix=''):
    size = long(size)

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

    if not Units.has_key(unit):
        return None

    else:
        return Units[unit](value)

class BackBlazeSession():
    def __init__(self, Email, Password, DiskIOEvent, TokenBucketInst, WorkingDirectory='temp', OutputDir='data', threads=32):
        self.__sess = requests.session()
        self.__Email = Email
        self.__Password = Password
        self.__SessionData = None
        self._WorkingDirectory = WorkingDirectory
        self._OutputDir = OutputDir
        self._threads = threads
        self._DiskIOEvent = DiskIOEvent
        self._TokenBucket = TokenBucketInst

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
        hashedstring = hashlib.sha1(text.encode('hex').lower()).hexdigest().lower()

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
            if self.__restore.has_key(item):
                return self.__restore[item]
            else:
                raise AttributeError

        def __getitem__(self, item):
            if self.__restore.has_key(item):
                return self.__restore[item]
            else:
                return AttributeError

        def iterkeys(self):
            return self.__restore.iterkeys()

        def keys(self):
            return self.__restore.keys()

        def __iter__(self):
            for key in self.__restore.iterkeys():
                yield key, self.__restore[key]

        def __repr__(self):
                if self.__restore.has_key('display_filename'):
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
                'hexemailaddr': self.__Email.encode('hex'),
                'request_firstbyteindex': Start,
                'request_numbytes': End,
                'hexpassword': 'Null'.encode('hex'),
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

                    data = ''.join(data)

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
            while pos < long(self.__restore['zipsize']):
                ChunksToDownload.append((pos, min(long(self.__restore['zipsize']), pos+0xffffff)))
                pos += 0xffffff

                #Tally up the total number of chunks
                TotalChunks += 1

            #Copy the total into chunks to download and then we will subtract from this when removing already downloaded data
            ToDownloadChunks = TotalChunks

            #First check if there is log for this restore and if there is load the list of chunks that have already been downloaded
            if os.path.exists(os.path.join(self.__parent._WorkingDirectory, self.__restore['rid'])):
                with open(os.path.join(self.__parent._WorkingDirectory, self.__restore['rid']), 'rb') as fil:
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
            
            #First check if the output file exists, if it does not then create it
            if not os.path.exists(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename'])):
                with open(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename']), 'wb') as fil:
                    pass

            #Now that the file exists open it for writing/modification
            outputfile = open(os.path.join(self.__parent._WorkingDirectory, self.__restore['display_filename']), 'rb+')

            #Open the log file so we can log after each piece is fetched so we may recover from a crash/power failure ect
            with open(os.path.join(self.__parent._WorkingDirectory, self.__restore['rid']), 'ab+') as logfile:
                #Define a list for keeping track of in progress downloads format is [(thread, starttime),]
                results = []               

                #Now lets start downloading missing pieces of the file until we have all pieces
                while len(ChunksToDownload) > 0 or len(results) > 0:
                    #Count the number of results that are ready and awaiting writing to disk, if its > than 40% then clear then clear the disk IO event to pause the zip extractor
                    #We are doing this when there are at least 40% of jobs waiting to write out as the thread pool is kept filled with jobs to 250% of running threads so that when a thread finishes 
                    #there is always another chunk waiting to start downloading immediatly and there is additional space for a number of completed jobs to sit in memory pending writing to disk
                    #40% allows for (Running Threads + (RunningThreads / 2 pending jobs)) + RunningThreads * 1.5 in output buffer before it triggers the zip extractor to stop
                    CompletedCount = 0
                    for job in results:
                        if job[0].ready():
                            CompletedCount += 1

                    if CompletedCount > len(results) * 0.4:
                        self.__parent._DiskIOEvent.clear()

                    #Iterate over the list of entrys in results list and retreive the output of any finished jobs then remove them from the list
                    for x in xrange(len(results) - 1, -1, -1):
                        #Track if any jobs have returned so that we can flush both the log file and the output file if we have written at least 1 chunk
                        AnyResultsFound = False

                        #If this job has finished executing
                        if results[x][0].ready():
                            result = results[x][0].get()

                            #If fetching the chunk failed, add it back into the list of missing chunks
                            if result[0] != True:
                                if not ChunkRetryCount.has_key((result[1], result[2])):                                                                       
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
                                outputfile.seek(result[1])
                                outputfile.write(result[3])

                                #And log that this chunk has been downloaded
                                logfile.write(struct.pack('QQ', result[1], result[2]))                               
                               
                                #Mark the disk buffer as requiring flushing, but only after all other results have been written
                                AnyResultsFound = True

                                #And increment downloaded chunks by 1
                                DownloadedChunks += 1     
                                ToDownloadChunks -= 1

                            #And remove the entry from the results list
                            del results[x]                      

                        #Flush the output file to disk followed by the log file to ensure we have a consistent state in the event the program is interupted
                        if AnyResultsFound:
                            outputfile.flush()                            
                            logfile.flush()

                    #Now after writing all chunks to disk, set the io event to enabled again to resume the zip extractor(if it was paused, this has no effect if we did not pause it)
                    self.__parent._DiskIOEvent.set()

                    #Make sure there are enough jobs queueed up to keep the thread pool busy, the larger
                    #This value the more data may be sitting in ram waiting being written out but the more consistent transfer speeds will be
                    while len(results) < (self.__parent._threads * 2.5) +1 and len(ChunksToDownload) > 0:
                        #Submit tasks to the threadpool to download all missing chunks, max concurrency is limited by pool size
                        for x in xrange(len(ChunksToDownload) -1, -1, -1):                             
                            results.append([self.__parent._tpool.apply_async(self.__DownloadPiece, (ChunksToDownload[x][0], ChunksToDownload[x][1],self.__parent._TokenBucket)), time.time()])

                            del ChunksToDownload[x]

                            #If the target backlog size has been reached, stop adding new tasks
                            if len(results) >= (self.__parent._threads * 2.5) +1:
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

            #Finally the file has finished downloading, close all open file handles and return the path of the output file
            outputfile.close()

            return os.path.join(WorkingDirectory, self.__restore['display_filename'])

    #Returns a list of restores, where each restore is an instance of the __Restore Object
    @property
    def Restores(self):
        if self.__SessionData == None:
            if not self.__Login():
                raise Exception('Unable To Login')

        resp = self.__sess.post('https://ca001.backblaze.com/api/restoreinfo', data={
            'version': '8.0.1.567',
            'hexemailaddr': self.__Email.encode('hex'),
            'hexpassword': 'Null'.encode('hex')
        }, headers={
            'Authorization': self.__SessionData['authToken'],
            'User-Agent': 'backblaze_agent/8.0.1.567'
        })

        #If this was successfull then iterate over the returned restores and yield each restore
        if resp.status_code == 200:
            tree = etree.fromstring(resp.content)

            for restore in tree.iter('restore'):
                RestoreEntry = {}

                for key in restore.keys():
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
                        with zipfil.open(zipfil.filelist[1].filename) as infile:
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
            self.__Queue = Queue.Queue()
            
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

    -threads - Sets how many threads should be run in parallel when downloading backups, default is 32.
    -zipregex=regularexpression,targetpath - This argument can be specified multiple times and overwrites the path that files will be extracted to for any files that match the provided regular expression
        Example: -zipregex=^c\\/users\\/test\\/.*,d:\\testuser would extract all files with the path c/users/test to d:\testusers, all other files would be put in the folder specified by -outputdir

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

    Extract = False
    KeepZip=True
    Throttle = False
    
    #Regular expressions to use when extracting the zip file, usefull for extracting different subfolders form the zipfile to different locations
    ZipRegex = []

    #Now parse any command line arguments, we want to accept arguments in either of 2 formats -arg=value or -arg value
    for x in xrange(0, len(sys.argv)):
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

    #Define an event that will be passed to both the downloader and the zipextractor if enabled, this allows the downloader to signal to the zip extractor that it needs the zip extractor to stop IO operations
    #This allows the downloader to temporally pause the zip extractor in the event its being bottlenecked by writing output chunks to disk due to the zip extractor maxing disk iops
    DiskIOEvent = threading.Event()
    DiskIOEvent.set()#When the event is set, the zip extractor will run, clearing the event will cause the zip extractor to pause until its set again

    #If extracting of zip files is enabled create a job queue for the zip extractor and then get an instance of the zip extract and start it
    if Extract:
        #Job queue that we will add the name of zip files to once they are ready to be extracted
        zipjobqueue = Queue.Queue()
        
        #And get an instance of the zip extractor and start it in its own thread, this allows the next zip file to begin downloading while the previous one is being extracted
        zipextrator = ZipExtrator(zipjobqueue, DiskIOEvent, ZipRegex, OutputDirectory, KeepZip)
        zipextrator.start()

    #If a throtte was specified, get a throttler object, if throttle is false this returns an object that always immediatly returns 
    TokenBucketInst = TokenBucket(Throttle)

    #If the user has not provided the bare minimum arguments, default to showing help
    if Email == None or Password == None or OutputDirectory == None:
        ShowHelp()
        raw_input('')
        exit(0)

    while True:        
        BB = BackBlazeSession(Email, Password, DiskIOEvent, TokenBucketInst, WorkingDirectory, OutputDirectory, threads)
        
        #Track if we have found a file to download in this iteration, if we have not then we will sleep for 15 minutes before checking again, otherwise check again immediatly
        FileFound = False

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
    
