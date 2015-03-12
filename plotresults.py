import os
import sys
import glob
from datetime import datetime
from datetime import timedelta
import pylab


BIN_SIZE = 10

MANIFEST_TYPE = 1
SEGMENT_TYPE = 2
BAD_TYPE = 3


class PlayerStats(object):
  def __init__(self, path):
    self._fpath = path
    self.start_time = None
    self.end_time = None
    self.bitrate = None
    self._time = []
    self._ftype = []
    self._content_length = []
    self._download_time = []
    self._buffer = []
    self._rebuf_count = []
    self._rebuf_dur = []
    self._rebuf_ratio = []
    self._error_count = []

  def get_time(self):
    return self._time

  def get_rebuf_dur(self):
    return self._rebuffer_duration

  def get_rebuf_ratio(self):
    return self._rebuf_ratio

  def get_download_time(self):
    return self._download_time

  def read_header(self):
    f = open(self._fpath, 'r')
    l = f.readline().strip()
    f.close()
    if not l:
      return []
    return l.split(',')

  def try_get_bitrate(self, url):
    query = url.split('?')[1]
    query_parts = query.split('&')
    for part in query_parts:
      if '=' not in part:
        continue
      k, v = part.split('=')
      if k == 'b':
        try:
          v = int(v)
        except Exception as e: # type error
          return None
        return int(v)
    # Did not find 'b=' in query string
    return None

  def parse(self):
    self._header = self.read_header()
    if not self._header:
      return
    f = open(self._fpath, 'r')
    idx = 0
    for l in f:
      idx += 1
      if idx == 1:
        continue
      vals = self.parse_line(l.strip(), self._header)
      if not vals: # bad line, skip!!
        continue
      if self.start_time is None:
        self.start_time = vals['time']
      if self.bitrate is None:
        self.bitrate = self.try_get_bitrate(vals['url'])
      self.end_time = vals['time']
      self._time.append(vals['time'])
      self._ftype.append(vals['type'])
      self._content_length.append(vals['content_length'])
      self._download_time.append(vals['download_time'])
      self._buffer.append(vals['buffer'])
      self._rebuf_count.append(vals['rebuf_count'])
      self._rebuf_dur.append(vals['rebuf_dur'])
      self._rebuf_ratio.append(vals['rebuf_ratio'])
      self._error_count.append(vals['error_count'])

  def cast_int(self, v):
    return int(v)

  def cast_float(self, v):
    return float(v)

  def cast_type(self, v):
    if v == 'manifest':
      return MANIFEST_TYPE
    elif v == 'seg':
      return SEGMENT_TYPE
    return BAD_TYPE

  def cast_time(self, v):
    # Expected format: 2015-03-03 16:02:51.347423
    return datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f') 

  def parse_line(self, l, header):
    vals = {}
    cast_options = {'time' : self.cast_time,
                    'type' : self.cast_type,
                    'content_length' : self.cast_int,
                    'download_time' : self.cast_float,
                    'buffer' : self.cast_float,
                    'rebuf_count' : self.cast_int,
                    'rebuf_dur' : self.cast_float,
                    'rebuf_ratio' : self.cast_float,
                    'error_count' : self.cast_int}
    try:
      parts = l.split(',')
      if len(parts) != len(header): # bad line!!
        return vals

      for i in range(len(parts)):
        if header[i] not in cast_options:
          vals[header[i]] = parts[i]
        else:
          v = cast_options[header[i]](parts[i])
          vals[header[i]] = v
    except Exception as err:
      print 'Error casting!!'
      return {}
    return vals

  def update_relative_time(self, ts):
    for i in range(len(self._time)):
      delta = self._time[i] - ts
      delta_sec = delta.seconds + delta.microseconds / 1000000.0
      self._time[i] = delta_sec


def parse_all_files(path):
  files = glob.glob(os.path.join(path, '*.csv'))
  all_players = []  
  start_time = None
  end_time = None

  for f in files:
    p = PlayerStats(f)
    p.parse()
    all_players.append(p)

    # update start_time, end_time
    if start_time is None:
      start_time = p.start_time
    else:
      if p.start_time < start_time:
        start_time = p.start_time

    if end_time is None:
      end_time = p.end_time
    else:
      if p.end_time > end_time:
        end_time = p.end_time
  return all_players, start_time, end_time 


def plot_num_players(player_count, path):
  t = pylab.arange(BIN_SIZE/2, len(player_count) * BIN_SIZE, BIN_SIZE)
  pylab.plot(t, player_count, linewidth=3.0)
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 1.1 * max(player_count)])
  pylab.title('Number of active players')
  pylab.xlabel('Time (s)')
  pylab.ylabel('Number of players')
  pylab.savefig(os.path.join(path, 'num_player.png'))
  pylab.close()


def plot_avg_bitrate(bitrates, path):
  t = pylab.arange(BIN_SIZE/2, len(bitrates) * BIN_SIZE, BIN_SIZE)
  pylab.plot(t, bitrates, linewidth=3.0)
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 100 + 1.1 * max(bitrates)])
  pylab.title('Average bitrate')
  pylab.xlabel('Time (s)')
  pylab.ylabel('Average video bitrate (Kbps)')
  pylab.savefig(os.path.join(path, 'avg_bitrate.png'))
  pylab.close()


def compute_rebuf_stats(rebuf_ratio):
  median = [0] * len(rebuf_ratio)
  perc95 = [0] * len(rebuf_ratio)
  for i in range(len(rebuf_ratio)):
    rebuf_ratio[i].sort()
    median[i] = rebuf_ratio[i][ len(rebuf_ratio[i])/2 ]
    perc95[i] = rebuf_ratio[i][ int(0.95 * len(rebuf_ratio[i])) ]
  return median, perc95


def plot_buf_ratio(rebuf_ratio, path):
  median, perc95 = compute_rebuf_stats(rebuf_ratio)
  t = pylab.arange(BIN_SIZE/2, len(rebuf_ratio) * BIN_SIZE, BIN_SIZE)
  pylab.plot(t, perc95, linewidth=3.0, label='Perc95')
  pylab.plot(t, median, linewidth=3.0, label='Median')
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 0.5+ 1.1 * max(perc95)])
  pylab.title('Rebuffering ratio (%) {median, perc95}')
  pylab.xlabel('Time (s)')
  pylab.ylabel('Rebuffering ratio (%)')
  legend = pylab.legend(loc='upper center', shadow=True)
  pylab.savefig(os.path.join(path, 'rebuf_ratio.png'))
  pylab.close()


def plot_summary(rebuf_ratio, bitrates, player_count, path):
  median, perc95 = compute_rebuf_stats(rebuf_ratio)
  t = pylab.arange(BIN_SIZE/2, len(rebuf_ratio) * BIN_SIZE, BIN_SIZE)
  pylab.figure(1)
  pylab.subplot(311)
  pylab.plot(t, player_count, linewidth=3.0)
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 1.1 * max(player_count)])
  pylab.ylabel('Number of players')

  pylab.subplot(312)
  pylab.plot(t, bitrates, linewidth=3.0)
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 100 + 1.1 * max(bitrates)])
  pylab.ylabel('Average bitrate (Kbps)')

  pylab.subplot(313)
  pylab.plot(t, perc95, linewidth=3.0, label='Perc95')
  pylab.plot(t, median, linewidth=3.0, label='Median')
  pylab.axis([0, max(t)+BIN_SIZE/2, 0, 0.5 + 1.1 * max(perc95)])
  pylab.xlabel('Time (s)')
  pylab.ylabel('Rebuffering ratio (%)')
  legend = pylab.legend(loc='upper center', shadow=True)
  pylab.savefig(os.path.join(path, 'summary.png'))
  pylab.close()


def plot_results(path):
  all_players, start_time, end_time = parse_all_files(path)
  duration = end_time - start_time
  duration_sec = duration.seconds + duration.microseconds / 1000000.0
  bucket_count = 1 + int(duration_sec / BIN_SIZE)

  request_count = [0] * bucket_count
  player_count = [0] * bucket_count
  rebuf_ratio = [[] for i in range(bucket_count)]
  bitrates = [0] * bucket_count

  for p in all_players:
    p.update_relative_time(start_time)
    time = p.get_time()
    this_rebuf = p.get_rebuf_ratio()

    for i in range(bucket_count):
      if time[0] < i * BIN_SIZE and time[-1] > (i+1) * BIN_SIZE:
        player_count[i] += 1
        if p.bitrate is not None:
          bitrates[i] += p.bitrate

    for i in range(len(time)):
      this_bucket = int(time[i] / BIN_SIZE)
      request_count[this_bucket] += 1
      rebuf_ratio[this_bucket].append(this_rebuf[i])

  # compute average bitrate
  for i in range(len(bitrates)):
    bitrates[i] = bitrates[i] / max(1, player_count[i])

  plot_num_players(player_count, path)
  plot_avg_bitrate(bitrates, path)
  plot_buf_ratio(rebuf_ratio, path)
  plot_summary(rebuf_ratio, bitrates, player_count, path)

