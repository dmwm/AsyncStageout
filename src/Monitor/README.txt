To run the monitor:

- set up your environment, take setup.sh for inspiration
- run ASO-Monitor.pl --config monitor.conf
- the logfile will be written to the path specified in the config file
- see monitor.conf for a list of configuration parameters and their meanings.

To run a fake generator for the Monitor input:
- set up the environment, as above
- run the monitor, as above
- run LifeCycle.pl --config fake-monitor-input.conf --log /path/to/log/file
- the fake-monitor-input.conf file is commented, you can tune the behaviour there

In both cases, if you alter the config file, the executable will re-read it on the fly.
