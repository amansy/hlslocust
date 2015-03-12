import urllib2
import socket
import urlparse
import time

import  cast
import hlserror


DOWNLOAD_TIMEOUT = 6


class HLSObject(object):
    def request(self, name=None):
        if name is None:
            name = self.url # I want to log full url
        self.content_len = 0
        try:
          r = urllib2.urlopen(self.url, timeout=DOWNLOAD_TIMEOUT)
        except (urllib2.HTTPError, urllib2.URLError) as e:
          self.bad_url = True
        except (socket.timeout, Exception) as e:
          pass
        else:
          try:
            self.content_len = int(dict(r.info().items())['content-length'])
          except KeyError:
            self.content_len = 0

          if self.content_len == 0:
            r = None
          return r
        return None

    def download(self):
      r = self.request()
      if r:
        self.parse(r.read())
        return True
      else:
        return False


class MasterPlaylist(HLSObject):
    def __init__(self,name,url,attributes=None):
        self.name=name
        self.url=url
        self.media_playlists = []
        if attributes:
            for k in attributes:
                setattr(self,k,attributes[k])

    def parse(self,manifest):
        self.media_playlists = []
        lines = manifest.split('\n')
        assert(lines[0].startswith('#EXTM3U'))

        for i,line in enumerate(lines):
            if line.startswith('#EXT-X-STREAM-INF'):
                key,val = line.split(':')
                attr = cast.my_cast(val)
                name = lines[i+1].rstrip() # next line
                url = urlparse.urljoin(self.url, name) # construct absolute url
                self.media_playlists.append(MediaPlaylist(name,url,attr))
            elif line.startswith('#EXT-X-'):
                try:
                    key,val = line.split(':')
                except ValueError:
                    key = line[:]
                    val = 'YES'
                key = cast.attr_name(key)
                val = cast.my_cast(val)
                setattr(self,key,val)


class MediaPlaylist(HLSObject):
    def __init__(self,name,url,attributes=None):
        self.name=name
        self.url=url
        self.media_fragments = []
        self.endlist = False
        if attributes:
            for k in attributes:
                setattr(self,k,attributes[k])

    def parse(self,manifest):
        ms_counter = None
        lines = manifest.split('\n')
        assert(lines[0].startswith('#EXTM3U'))
        for i,line in enumerate(lines):
            if line.startswith('#EXTINF'):
                key,val = line.split(':')
                attr = cast.my_cast(val)
                name = lines[i+1].rstrip() # next line
                if not ms_counter:  #
                    try:
                        ms_counter = self.media_sequence  # probably live
                    except AttributeError:
                        ms_counter = 1  # probably VOD
                if not name.startswith('#'):
                    # TODO, bit of a hack here. Some manifests put an attribute
                    # line on the first fragment which breaks this.
                    if ms_counter > self.last_media_sequence():
                        url = urlparse.urljoin(self.url, name) # construct absolute url
                        self.media_fragments.append(MediaFragment(name,
                                                                  url,
                                                                  attr,
                                                                  self,
                                                                  ms_counter))
                ms_counter += 1

            elif line.startswith('#EXT-X-ENDLIST'):
              self.endlist = True
              return

            elif line.startswith('#EXT-X-'):
                try:
                    key,val = line.split(':')
                except ValueError:
                    key = line[:]
                    val = 'YES'
                key = cast.attr_name(key)
                val = cast.my_cast(val)
                setattr(self,key,val)

    def first_media_sequence(self):
        try:
            return self.media_fragments[0].media_sequence
        except IndexError:
            return -1

    def last_media_sequence(self):
        try:
            return self.media_fragments[-1].media_sequence
        except IndexError:
            return -1

    def get_media_fragment(self, msq):
        idx = msq - self.first_media_sequence()
        idx = max(idx, 0)
        idx = min(idx, len(self.media_fragments)-1)
        if self.media_fragments[idx].media_sequence != msq:
            raise hlserror.MissedFragment('Fragments are not numbered '
                                          'sequentially: {0}!={1}'.format(
                                          self.media_fragments[idx].media_sequence,
                                          msq))
        return self.media_fragments[idx]

class MediaFragment(HLSObject):
    def __init__(self,name,url,attributes,parent=None, seq=None):
        self.url=url
        self.name=name
        self.parent = parent
        self.duration = attributes[0] # only attrib??
        self.media_sequence = seq

    def download(self):
        #assert(str(self.media_sequence) in self.name) # HACK
        name = 'Segment ({url})'.format(url=self.parent.url)
        r = self.request(name=name)
        if r:
            return True
        else:
            return False

