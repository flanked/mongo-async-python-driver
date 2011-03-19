import os.path
import mimetypes
mimetypes.init()
mimetypes.add_type('image/x-dwg', '.dwg')
mimetypes.add_type('image/x-icon', '.ico')
mimetypes.add_type('application/x-bzip2', '.bz2')
mimetypes.add_type('audio/x-m4a', '.m4a')
mimetypes.add_type('audio/x-musepack', '.mpc')
mimetypes.add_type('audio/x-wavpack', '.wv')
mimetypes.add_type('video/mp4', '.mp4')
mimetypes.add_type('video/mpegts', '.ts')
mimetypes.add_type('video/divx', '.divx')
mimetypes.add_type('video/divx', '.avi')
mimetypes.add_type('video/x-matroska', '.mkv')

import zope.interface
from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.python import log
from twisted.web import server
from twisted.web.static import (StaticProducer, NoRangeStaticProducer, 
                                SingleRangeStaticProducer, 
                                MultipleRangeStaticProducer, File, Registry)

def get_mimetype(ext, default='applications/x-download'):
    return mimetypes.types_map.get(ext, default)

class GridFsFile(Resource):
    """ 
    GridFsFile is a Twisted File resource for transfering GridFS files
    over the network using a non-blocking producer.
    """
    isLeaf = True
    def __init__(self, gridfs, filename=None):
        self.filename = filename
        self.gridfs = gridfs   
        
    def getChild(self, path, request):
        if self.filename is not None:
            return self.filename
        return path[0]         
    
    def render_HEAD(self, filename):
        return self.render_GET(video)
    
    def render_GET(self, request):
        """ Get on the root index """
        def _success(doc, request, filename, include_body):
            _, ext = os.path.splitext(filename)
            ct = doc.content_type
            if not ct:
                ct = get_mimetype(ext)
            
            request.setHeader('Content-Type', ct)
            
            request.setHeader('Last-Modified', doc.upload_date)
            
            if "v" in request.args:
                request.setHeader("Expires", datetime.datetime.utcnow() + \
                                           datetime.timedelta(days=365*10))
                request.setHeader("Cache-Control", "max-age=" + str(86400*365*10))
            else:
                request.setHeader("Cache-Control", "public")
            
            
            cached = False
            ims_value = request.getHeader("If-Modified-Since")
            if ims_value is not None:
                date_tuple = email.utils.parsedate(ims_value)
                if_since = datetime.datetime.fromtimestamp(time.mktime(date_tuple))
                if if_since >= modified:
                    request.set_status(304)
                    request.finish()
                    cached = True                          
            
            if not cached and request.method != 'HEAD':
                request.setHeader('Content-Length', doc.length)
            
#                    self.set_header('Content-Disposition',
#                                    'attachment; filename=%s'%video)
                try:
                    producer = GridFsNoRangeStaticProducer(request, doc)
                    producer.start()                        
                except IOError,e:
                    log.err("Failed to read the file: %s")            
        
        def _failure(failure):
            log.err(failure)
            request.setResponseCode(404)
            request.finish()
            return ''
        
        filename = self.getChild(request.postpath, request)        
        d = self.gridfs.get_last_version(filename)
        d.addCallback(_success, request, filename, request.method != 'HEAD')
        d.addErrback(_failure)
        return server.NOT_DONE_YET


class GridFsStaticMixin(object):
    """ Mixin for a static file producing with GridFile read compatibility. """    
    # Default chunksize for GridFS
    bufferSize = 2**18
    
    @defer.inlineCallbacks
    def resumeProducing(self):
        if not self.request:
            log.error("Resuming a producer with no request object")
            return
        data = yield self.fileObject.read(
            min(self.bufferSize, self.size - self.bytesWritten))
        if data:
            self.request.write(data)
            self.bytesWritten += len(data)
        else:
            log.err("Finished reading from grid file: %s" % self.fileObject)
            self.request.unregisterProducer()
            self.request.finish()
            self.stopProducing()


class GridFsNoRangeStaticProducer(GridFsStaticMixin, NoRangeStaticProducer):
    """
    A GridFS Static Producer that writes the entire file to the request.
    """
    def __init__(self, request, fileObject):
        NoRangeStaticProducer.__init__(self, request, fileObject)
        # Add bytesWritten for simplicity
        self.bytesWritten = 0
        self.size = fileObject.length
    

class GridFsSingleRangeStaticProducer(GridFsStaticMixin, 
                                      SingleRangeStaticProducer):
    """
    A GridFS Static Producer that writes a single chunk of a file to
    the request.
    """
    def __init__(self, request, fileObject, offset, size):
        SingleRangeStaticProducer.__init__(self, request, fileObject, offset, 
                                           size)
        self.bytesWritten = 0


class GridFsMultipleRangeStaticProducer(GridFsStaticMixin, 
                                        MultipleRangeStaticProducer):
    """
    A GridFS Static Producer that writes a several chunks of a file to
    the request.
    """
    
    @defer.inlineCallbacks
    def resumeProducing(self):
        """ Resume producing a multi-range file.
        
            Multi-range producers require more hand-holding to stream
            multiple chunks of a file than a simple GridFsStaticMixin can
            provide. We keep the Mixin inheritance for consistancy.
        """
        if not self.request:
            return
        data = []
        dataLength = 0
        done = False
        while dataLength < self.bufferSize:
            if self.partBoundary:
                dataLength += len(self.partBoundary)
                data.append(self.partBoundary)
                self.partBoundary = None
            p = yield self.fileObject.read(
                min(self.bufferSize - dataLength,
                    self._partSize - self._partBytesWritten))
            self._partBytesWritten += len(p)
            dataLength += len(p)
            data.append(p)
            if self.request and self._partBytesWritten == self._partSize:
                try:
                    self._nextRange()
                except StopIteration:
                    done = True
                    break
        self.request.write(''.join(data))
        if done:
            self.request.unregisterProducer()
            self.request.finish()
            self.request = None