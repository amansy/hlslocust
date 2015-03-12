hlsplayer
=========

Python based HLS player that can play Live and VOD content, simulates player buffer, and exports multiple statistics


Usage
-----

```
python hlsplayer.py --help

usage: hlsplayer.py [-h] [--url url] [-d DUR] [-n NUM_PLAYERS] [-r RATE]
                    [--dst DST_DIR]
```

optional arguments:

*  -h, --help                   show this help message and exit

*  --url url                            URL of master manifest (or playlist)

*  -d DUR, --duration DUR       duration of the streaming session

*  -n NUM_PLAYERS               Number of HLS players to simulate

*  -r RATE, --rate RATE         Rate of new HLS players per second

*  --dst DST_DIR                        path where log files will be saved


Note that a new thread is created for each player, so this script is not suitable for high performance load testing. The player does NOT implement adaptation logic. This mean that if given a master playlist, the player randomly picks one of the available bitrates and sticks to it until the end of the streaming session. When having multiple players, however, each player makes that decision independently. The player simulates the video buffer behavior and computes rebuffering events. The script creates a new directory named 'expXXX' where XXX is a three digit number that represents the experiment number. All log files and generated plots are written to that directory. 

