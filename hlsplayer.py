import os, sys, signal, time
import argparse
import logging

from threading import Thread, Lock
from datetime import datetime
from datetime import timedelta
import urllib2
import socket

import hlsobject
import hlserror
import plotresults


NUM_DOWNLOAD_RETRIES = 5
DOWNLOAD_TIMEOUT = 6
MANIFEST_TIMEOUT = 6
BUFFER_FILL_LEVEL = 25


CSV_HEADER="time,type,content_length,download_time,buffer,rebuf_count,rebuf_dur,rebuf_ratio,error_count,player_id,url"


# flag to stop execution
should_exit = False



class Player(Thread):
  def __init__(self, dur, dst_dir, url):
    Thread.__init__(self)
    self._url = url
    self._dur = dur
    self._last_sequence = -1
    self._last_pl = None
    self._seg_size = -1
    self._last_update_time = datetime.now()
    self._start_time = datetime.now()
    self._buffer = 0.0
    self._playing = False
    self._rebuffer_count = 0
    self._rebuffer_duration = 0.0
    self._rebuf_ratio = 0.0
    self._download_error_count = 0
    self._logfile = None
    self.open_log_file(dst_dir)

  def open_log_file(self, dst_dir):
    # Create log file: file name is a the current timestamp
    self._player_id = time.time() * 1000000
    log_fname = "%d.csv" % self._player_id
    fout_path = os.path.join(dst_dir, log_fname)
    # Make sure we don't have another player with the same id
    while os.path.exists(fout_path):
      self._player_id += 1
      log_fname = "%d.csv" % self._player_id
      fout_path = os.path.join(dst_dir, log_fname)
    self._logfile = open(fout_path, 'w')
    # write csv header line
    self._logfile.write('%s\n' % CSV_HEADER)

  def get_master_playlist(self):
    ts_start = datetime.now()
    for i in range(NUM_DOWNLOAD_RETRIES):
      self.master_playlist = hlsobject.MasterPlaylist('master', self._url)
      r = self.master_playlist.download()
      if r is True:
        break
    return ts_start, datetime.now()

  def download(self, obj):
    ts_start = datetime.now()
    for i in range(NUM_DOWNLOAD_RETRIES):
      r = obj.download()
      if r is True:
        break
    return ts_start, datetime.now(), r

  def log_msg(self, msg):
    self._logfile.write("%s\n" % msg)

  def update_player(self, new_seg, ts, seglen=0):
    if not self._playing:
      if not new_seg:
        return
      # start playing video, update current time 
      self._playing = True
      self._last_update_time = ts
      self._start_time = ts
    delta_ts = ts - self._last_update_time
    delta_sec = delta_ts.seconds + delta_ts.microseconds / 1000000.0
    if delta_sec > self._buffer:
      # rebuffering event happened, buffer should be empty now
      if self._buffer > 0.1:
        # update rebuffer count only when entering a new rebuffering state 
        self._rebuffer_count = self._rebuffer_count + 1
      self._rebuffer_duration = self._rebuffer_duration + (delta_sec - self._buffer)
      self._buffer = 0.0
    else:
      # reducing buffer size
      self._buffer = self._buffer - delta_sec
    # Increase buffer if received a new segment
    if new_seg:
      self._buffer = self._buffer + seglen 
    # update rebuf ratio, first compute current streaming duration
    if self._rebuffer_duration > 0.0:
      dur = ts - self._start_time
      dur_sec = dur.seconds + dur.microseconds / 1000000.0
      self._rebuf_ratio = self._rebuffer_duration / dur_sec
    self._last_update_time = ts

  def log_file_download(self, f_type, url, ts_start, ts_end, content_len):
    delta = ts_end - ts_start
    delta_sec = delta.seconds + (delta.microseconds / 1000000.0) 
    self.log_msg('%s,%s,%d,%f,%f,%d,%f,%f,%d,%d,%s' % (
                                                str(ts_start),
                                                f_type,
                                                content_len,
                                                delta_sec * 1000.0,
                                                self._buffer,
                                                self._rebuffer_count,
                                                self._rebuffer_duration,
                                                self._rebuf_ratio * 100,
                                                self._download_error_count,
                                                self._player_id,
                                                url
                                                ))

  def run(self):
    # download initial playlist
    ts = datetime.now()
    self._last_update_time = ts
    dur_sec = 0.0

    # first, download the master manifest, randomly pock one of the bitrates
    ts_start, ts_end = self.get_master_playlist()
    if len(self.master_playlist.media_playlists) == 0:
      # no master manifest, probably a stream with a single bitrate
      playlist = hlsobject.MediaPlaylist('media', self._url)
    else:
      playlist = random.choice(self.master_playlist.media_playlists)

    ts_start, ts_end, r = self.download(playlist)
    if r is False:
      logging.error('Player %d: Bad manifest, exiting...' % self._player_id)
      return
    self.log_file_download('manifest', playlist.url, ts_start, ts_end, playlist.content_len)
    playlist_download_time = ts_end

    if playlist.endlist:        # VOD
      media_seq = playlist.first_media_sequence()
    else: # live video
      media_seq = max(playlist.last_media_sequence() - 2, playlist.first_media_sequence())

    while not should_exit and dur_sec < self._dur:

      if media_seq <= playlist.last_media_sequence():
        try:
          a = playlist.get_media_fragment(media_seq)
          ts_start, ts_end, r = self.download(a)
          self.update_player(r, ts_end, a.duration)
          self.log_file_download('seg', a.url, ts_start, ts_end, a.content_len)
        except hlserror.MissedFragment as e:
          pass
        media_seq += 1

      # Check if we need to refresh the playlist (in case of Live)
      #   1) ENDLIST tag does not exist
      #   2) We have not reached the end of the current playlist yet
      # Note that we should refresh the playlist ONLY after Sometime has passed
      #  since downloading the previous one
      if not playlist.endlist and media_seq > playlist.last_media_sequence():
        playlist_age = datetime.now() - playlist_download_time
        playlist_age_sec = playlist_age.seconds + playlist_age.microseconds / 1000000.0
        while playlist_age_sec < a.duration:
          time.sleep(1)
          if should_exit:
            break
          playlist_age = datetime.now() - playlist_download_time
          playlist_age_sec = playlist_age.seconds + playlist_age.microseconds / 1000000.0
        ts_start, ts_end, r = self.download(playlist)
        # Update playlist download time only if we get new segments
        if playlist.last_media_sequence() >= media_seq:
          playlist_download_time = ts_end
        self.update_player(False, ts_end)
        self.log_file_download('manifest', playlist.url, ts_start, ts_end,
            playlist.content_len)

      # Check if we are done playing a VOD video
      if playlist.endlist and media_seq > playlist.last_media_sequence():
        break
    
      # Player will sleep as long as the buffer is above a certain threshold
      while self._buffer > BUFFER_FILL_LEVEL:
        time.sleep(1)
        if should_exit:
          break
        self.update_player(False, datetime.now())

      # Update video playout duration
      dur = datetime.now() - self._start_time
      dur_sec = dur.seconds + dur.microseconds / 1000000.0


def parse_params():
  parser = argparse.ArgumentParser(description='Simulate HLS player')
  parser.add_argument('--url', metavar='url', type=str, default="", dest='url',
                       help='URL of master manifest (or playlist)')

  parser.add_argument('-d', '--duration', dest='dur', default=60, type=int,
                      help='duration of the streaming session')

  parser.add_argument('-n', '--num_players', dest='num_players', default=1,
                      type=int, help='Number of HLS players to simulate')

  parser.add_argument('-r', '--rate', dest='rate', default=1.0,
                      type=float, help='Rate of new HLS players per second')

  parser.add_argument('--dst', dest='dst_dir', default="./", type=str,
                      help='path where log files will be saved')
  return parser


def signal_handler(signum, frame):
  global should_exit
  print 'Interrupted, exiting....'
  should_exit = True


def create_experiment_dir(dst_dir):
  exp_id = 1
  found_new_name = True
  new_dir = os.path.join(dst_dir, 'exp%03d' % exp_id)
  while os.path.exists(new_dir):
    exp_id = (exp_id + 1) % 1000
    if exp_id == 0: # was not able to create a directory
      found_new_name = False
      break
    new_dir = os.path.join(dst_dir, 'exp%03d' % exp_id)
  if found_new_name:
    os.makedirs(new_dir)
    return new_dir
  return dst_dir # return base directory if failed to create a new one



def main(argv):
  logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

  parser = parse_params()
  args = parser.parse_args()
  bad_args = False

  if not args.url:
    logging.error('Empty url ... exiting')
    bad_args = True
  elif args.num_players <= 0:
    logging.error('Number of players myst be positive, exiting...')
    bad_args = True
  elif args.rate <= 0:
    logging.error('Rate of clients must be positive, exiting...')
    bad_args = True
    
  if bad_args:
    parser.print_help()
    return

  url = args.url
  dur = args.dur
  n = args.num_players
  rate = args.rate
  dst_dir = args.dst_dir

  if not os.path.exists(dst_dir):
    logging.error('Destination path %s does not exist, you should create the path, exiting!' % dst_dir)
    return
  # Create a subdirectory for this experiment
  dst_dir = create_experiment_dir(dst_dir)

  logging.info("Starting HLS player(s) ...")
  players = []
  for i in range(n):
    p = Player(dur, dst_dir, url)
    players.append(p)
    p.start()
    time.sleep(1.0 / rate)

  logging.info("Started all player(s) ...")

  while True:
    try:
      time.sleep(2)
      all_players_finished = True
      for p in players:
        if p.isAlive():
          all_players_finished = False
          break
      if all_players_finished:
        break
    except (KeyboardInterrupt, SystemExit):
      print 'Received keyboad interrupt, exiting'
  return dst_dir


for sig in [signal.SIGTERM, signal.SIGINT]:
  signal.signal(sig, signal_handler)


path = main(sys.argv)

plotresults.plot_results(path)

