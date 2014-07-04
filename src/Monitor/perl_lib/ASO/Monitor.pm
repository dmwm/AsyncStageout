package ASO::Monitor;
use strict;
use warnings;

use base 'PHEDEX::Core::Logging';
use base 'ASO::JobManager';
use POE;
use POE::Queue::Array;
use JSON::XS;
use POSIX;
use File::Path;
use ASO::Job;
use ASO::File;
use ASO::GliteAsync;
use PHEDEX::Monitoring::Process;

use Data::Dumper;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my ($self,$help,%params,%args);
  %args = @_;
  map { $args{uc $_} = delete $args{$_} } keys %args;
  %params = (
          CONFIG		 => undef,
	  CONFIG_POLL		 => 11,
          INBOX			 => undef,
          OUTBOX		 => undef,
          WORKDIR		 => undef,
          SERVICE		 => undef,
          BASE_PRIORITY		 => 100,
          VERBOSE		 => 0,
          DEBUG			 => 0,
	  LOGFILE		 => undef,

	  JOBMANAGER		 => undef,
	  JOB_PARALLELISM	 =>  4,       # Max number of monitoring jobs to run in parallel
	  DEBUG_JOBS		 => undef,    # set true for debugging the job-manager

          Q_INTERFACE		 => undef,    # A transfer queue interface object
          Q_TIMEOUT		 => 60,       # Timeout for Q_INTERFACE commands
          INBOX_POLL_INTERVAL	 => 10,       # Inbox polling interval
          JOB_POLL_INTERVAL_SLOW => 10,       # Job polling interval
          JOB_POLL_INTERVAL_FAST =>  1,       # Job polling interval
          POLL_QUEUE		 =>  0,       # Poll the queue or not?
          ME                     => 'ASOMon', # Arbitrary name for this object
          STATISTICS_INTERVAL    => 900,      # Interval for reporting statistics
          QUEUE			 => undef,    # A POE::Queue of transfer jobs...
          JOBS			 => {},       # A hash of Job-IDs.
          LAST_SUCCESSFULL_POLL  => time,     # When I last got a job status
          QUEUE_STATS_INTERVAL   => 60,       # How often do I report the job-queue length
          REPORTER_INTERVAL	 => 30,       # How often to notify the Reporter of progress

	  FORGET_JOB		 => 60,       # Timer for internal cleanup
	  FILE_TIMEOUT		 => undef,    # Timeout for file state-changes
	  JOB_TIMEOUT		 => undef,    # Global job timeout
	  KEEP_INPUTS		 => 0,        # Set non-zero to keep the input JSON files
        );
  $self = \%params;
  bless $self, $class;

# Become a daemon, if that's what the user wants.

  if ( ! defined ($self->{CONFIG}=$args{CONFIG}) ) {
    die "No --config file specified!\n";
  }
  $self->ReadConfig();
  map { $self->{uc $_} = $args{$_} if $args{$_} } keys %args;
  if ( $self->{LOGFILE} && ! $self->{PIDFILE} ) {
    $self->{PIDFILE} = $self->{LOGFILE};
    $self->{PIDFILE} =~ s%.log$%%;
    $self->{PIDFILE} .= '.pid';
  }
  $self->daemon() if $self->{LOGFILE};

  $self->{JOBMANAGER} = ASO::JobManager->new(
		  KEEPALIVE => 61,
		  NJOBS     => $self->{JOB_PARALLELISM},
		  DEBUG     => $self->{DEBUG_JOBS},
		);

  $self->{QUEUE} = POE::Queue::Array->new();
  $self->{Q_INTERFACE} = ASO::GliteAsync->new
                (  
                  SERVICE => $self->{SERVICE},
                  ME      => 'GLite',
                  VERBOSE => $self->{VERBOSE},
                  DEBUG   => $self->{DEBUG},
		  WRAPPER => $self->{FAKE_TRANSFER_SCRIPT},
                );

  POE::Session->create(
    object_states => [
      $self => {
        poll_inbox		=> 'poll_inbox',
        _default		=> '_default',
        _start			=> '_start',
        _child			=> '_child',

        poll_job		=> 'poll_job',
        poll_job_postback	=> 'poll_job_postback',
        report_job		=> 'report_job',
        forget_job		=> 'forget_job',
        report_queue		=> 'report_queue',
        notify_reporter		=> 'notify_reporter',

        make_stats		=> 'make_stats',
        re_read_config		=> 're_read_config',

      },
    ],
  );

# Sanity checks:
  $self->{JOB_POLL_INTERVAL_FAST}>0.01 or die "JOB_POLL_INTERVAL_FAST too small:",$self->{JOB_POLL_INTERVAL_FAST},"\n";
  ref($self->{Q_INTERFACE}) or die "No sensible Q_INTERFACE object defined.\n";

  foreach ( keys %params ) {
    next if $_ eq 'JOB_TIMEOUT';
    next if $_ eq 'FILE_TIMEOUT';
    next if $_ eq 'LOGFILE';
    next if $_ eq 'DEBUG_JOBS';
    defined $self->{$_} or die "Fatal: $_ not defined after reading config file\n";
  }

  foreach ( qw / INBOX WORKDIR OUTBOX / ) {
    -d $self->{$_} or die "$_ directory $self->{$_}: Non-existant or not a directory\n";
  }

# Finally, a little self-monitoring
  $self->{pmon} = PHEDEX::Monitoring::Process->new();

  return $self;
}

sub daemon {
  my ($self, $me) = @_;
  my $pid;

  # Open the pid file.
  open(PIDFILE, "> $self->{PIDFILE}")
      || die "$me: fatal error: cannot write to PID file ($self->{PIDFILE}): $!\n";
  $me = $self->{ME} unless $me;

  return if $self->{NODAEMON};

  # Fork once to go to background
  die "failed to fork into background: $!\n"
      if ! defined ($pid = fork());
  close STDERR if $pid; # Hack to suppress misleading POE kernel warning
  exit(0) if $pid;

  # Make a new session
  die "failed to set session id: $!\n"
      if ! defined setsid();

  # Fork another time to avoid reacquiring a controlling terminal
  die "failed to fork into background: $!\n"
      if ! defined ($pid = fork());
  close STDERR if $pid; # Hack to suppress misleading POE kernel warning
  exit(0) if $pid;

  # Clear umask
  # umask(0);

  # Write our pid to the pid file while we still have the output.
  ((print PIDFILE "$$\n") && close(PIDFILE))
      or die "$me: fatal error: cannot write to $self->{PIDFILE}: $!\n";

  # Indicate we've started
  print "$me: pid $$", ( $self->{DROPDIR} ? " started in $self->{DROPDIR}" : '' ), "\n";

  print "writing logfile to $self->{LOGFILE}\n";
  # Close/redirect file descriptors
  $self->{LOGFILE} = "/dev/null" if ! defined $self->{LOGFILE};
  open (STDOUT, ">> $self->{LOGFILE}")
      or die "$me: cannot redirect output to $self->{LOGFILE}: $!\n";
  open (STDERR, ">&STDOUT")
      or die "Can't dup STDOUT: $!";
  open (STDIN, "</dev/null");
  $|=1; # Flush output line-by-line
}

sub re_read_config {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  if ( defined($self->{mtime}) ) {
    my $mtime = (stat($self->{CONFIG}))[9];
    if ( $mtime > $self->{mtime} ) {
      $self->Logmsg("Config file has changed, re-reading...");
      $self->ReadConfig();
      $self->{mtime} = $mtime;
    }
  } else {
    $self->{mtime} = (stat($self->{CONFIG}))[9] or 0;
  }

  $kernel->delay_set('re_read_config',$self->{CONFIG_POLL});
}

sub ReadConfig {
  my $self = shift;

  $self->Logmsg("Reading config file $self->{CONFIG}");
  open CONFIG, "<$self->{CONFIG}" or die "Cannot open config file $self->{CONFIG}: $!\n";

  while ( <CONFIG> ) {
    next if m%^\s*#%;
    next if m%^\s*$%;
    s%#.*$%%;

    next unless m%\s*(\S+)\s*=\s*(\S+)\s*$%;
    $self->{uc $1} = $2;
  }
  close CONFIG;

  $self->Logmsg("Using FTS service ",$self->{SERVICE}) if $self->{VERBOSE};
  if ( $self->{FAKE_TRANSFER_SCRIPT} ) {
    $self->Logmsg("Faking transfers with ",$self->{FAKE_TRANSFER_SCRIPT});
  }
  if ( $self->{FAKE_TRANSFER_RATE} ) {
    my $rate = $self->{FAKE_TRANSFER_RATE}; my $units = 'B/s';
    if ( $rate > 10240 ) { $rate /= 1024; $units = 'kB/s'; }
    if ( $rate > 10240 ) { $rate /= 1024; $units = 'MB/s'; }
    if ( $rate > 10240 ) { $rate /= 1024; $units = 'GB/s'; }
    if ( $rate > 10240 ) { $rate /= 1024; $units = 'TB/s'; }
    if ( $rate > 10240 ) { $rate /= 1024; $units = 'PB/s'; }
    $rate = int(10*$rate)/10;
    $self->Logmsg("Faking transfers rate: ",$rate,' ',$units) if $self->{VERBOSE};
  }

  if ( $self->{FAKE_TRANSFER_SCRIPT} ) {
    $self->{Q_INTERFACE}{WRAPPER} = $self->{FAKE_TRANSFER_SCRIPT};
  }
  if ( $self->{JOBMANAGER} ) {
    $self->{JOBMANAGER}{NJOBS} = $self->{JOB_PARALLELISM};
    $self->{JOBMANAGER}{DEBUG} = $self->{DEBUG_JOBS};
  }
}

sub _default {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  my $ref = ref($self);
  die <<EOF;

  Default handler for class $ref:
  The default handler caught an unhandled "$_[ARG0]" event.
  The $_[ARG0] event was given these parameters: @{$_[ARG1]}

  (...end of dump)
EOF
}

sub _start {
  my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION ];

  my $poll_job_postback  = $session->postback( 'poll_job_postback'  );
  $self->{POLL_JOB_POSTBACK} = $poll_job_postback;

  $kernel->delay_set('poll_job',$self->{JOB_POLL_INTERVAL_SLOW})
        if $self->{Q_INTERFACE}->can('ListJob');
  $kernel->delay_set('report_queue',$self->{QUEUE_STATS_INTERVAL});
  $kernel->delay_set('notify_reporter',$self->{REPORTER_INTERVAL});
  $kernel->yield('poll_inbox');
  $self->read_directory($self->{WORKDIR});

  $kernel->delay_set('make_stats',$self->{STATISTICS_INTERVAL}) if $self->{STATISTICS_INTERVAL};
  $kernel->delay_set('re_read_config',$self->{CONFIG_POLL});
}

sub make_stats
{
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

  my $summary = 'AGENT_STATISTICS ' . $self->{pmon}->FormatStats($self->{pmon}->ReadProcessStats);
  $self->Logmsg($summary);

  $kernel->delay_set('make_stats',$self->{STATISTICS_INTERVAL});
}

sub poll_inbox {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

  $self->Dbgmsg("Polling inbox") if $self->{DEBUG};
  $self->read_directory($self->{INBOX});

  $kernel->delay_set('poll_inbox',$self->{INBOX_POLL_INTERVAL});
}

sub read_directory {
  my ($self,$pattern) = @_;
  my ($location,$h,$job,$json,$file,@files);

  $pattern = $self->{INBOX} unless $pattern;
  if ( $pattern =~ m%$self->{INBOX}% ) {
    $location = 'inbox';
  } else {
    $location = 'work directory';
  }
  $pattern = $pattern . '/Monitor\.[0-9,a-f,-]*\.json';
  @files = glob( $pattern );

  $self->Logmsg("Found ",scalar @files," new items in ",$location) if $self->{VERBOSE};
  foreach $file ( sort @files ) {
    $self->Dbgmsg("Reading ",$file) if $self->{DEBUG};
    open JSON, "<$file" or die "Error reading $file: $!\n";
    $json = <JSON>;
    close JSON;
    eval {
      $h = decode_json($json);
    };
    if ( $@ ) {
      $self->Logmsg("Warning: Could not parse $file from inbox");
      unlink $file;
    }

    $job = ASO::Job->new(
	  ID		  => $h->{FTSJobid},
	  STATE		  => 'undefined',
	  SERVICE	  => $self->{SERVICE},
	  TIMESTAMP	  => time,
	  TIMEOUT	  => $self->{JOB_TIMEOUT},
	  FILE_TIMEOUT    => $self->{FILE_TIMEOUT},
	  VERBOSE	  => 1,
	  X509_USER_PROXY => $h->{userProxyPath},
	  USERNAME        => $h->{username},
	);
    my (@Files,$i,$lenPFNs);
    $lenPFNs = scalar @{$h->{PFNs}};
    for ($i=0; $i<$lenPFNs; ++$i) {
      push @Files, ASO::File->new(
	  SOURCE	=> $h->{PFNs}[$i],
	  DESTINATION	=> 'dummy'
	);
      $self->{FN_MAP}{$h->{PFNs}[$i]} = $h->{LFNs}[$i];
    }
    $job->Files( @Files );

    $self->QueueJob($job,$self->{BASE_PRIORITY});
    my $tmp;
    ($tmp = $file) =~s%^.*/%%;
    rename $file, $self->{WORKDIR} . '/' . $tmp;
  }
}

sub _child {}

sub report_queue {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  my ($priority,$id,$job);

  $self->Logmsg("Job queue: ",$self->{QUEUE}->get_item_count()," items");
  $kernel->delay_set('report_queue',$self->{QUEUE_STATS_INTERVAL});
}

sub poll_job {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  my ($priority,$id,$job,$logfile);

  while ( $self->{JOBMANAGER}->jobsQueued() < $self->{JOB_PARALLELISM} ) {
    ($priority,$id,$job) = $self->{QUEUE}->dequeue_next;
    if ( $id ) {
      $self->Logmsg('dequeue JOBID=',$job->ID) if $self->{VERBOSE};
      $logfile = '/dev/null';
      if ( $self->{JOBLOGS} ) {
        $logfile = $self->{JOBLOGS} . '/' . $job->ID . '.' . time() . '.log';
      }
      $self->{JOBMANAGER}->addJob(
		      $self->{POLL_JOB_POSTBACK},
		      {
		        FTSJOB => $job,
		        LOGFILE => $logfile,
		        KEEP_OUTPUT => 1,
		        TIMEOUT => $self->{Q_TIMEOUT},
		        ENV => {
		          PHEDEX_FAKEFTS_RATE => $self->{FAKE_TRANSFER_RATE},
		          PHEDEX_FAKEFTS_MAP  => $self->{FAKE_TRANSFER_MAP},
		          X509_USER_PROXY     => $job->{X509_USER_PROXY},
		        },
		      },
		      $self->{Q_INTERFACE}->Command('ListJob',$job)
		    );
    } else {
      $self->{LAST_SUCCESSFUL_POLL} = time;
      $kernel->delay_set('poll_job', $self->{JOB_POLL_INTERVAL_FAST});
      return;
    }
  }
  $kernel->delay_set('poll_job', $self->{JOB_POLL_INTERVAL_SLOW});
}

sub poll_job_postback {
  my ( $self, $kernel, $arg0, $arg1 ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
  my ($result,$priority,$id,$job,$summary,$command,$error);

  $error = '';
  $command = $arg1->[0];
  if ($command->{STATUS} ne "0") { 
      $error = "ended with status $command->{STATUS}";
      if ($command->{STDERR}) { 
          $error .= " and error message: $command->{STDERR}"; 
      }
  }

  $job = $command->{FTSJOB};
  $result = $self->{Q_INTERFACE}->ParseListJob( $job, $command->{STDOUT} );

  if ( $self->{DEBUG} && $command->{DURATION} > 8 )
  {
    my $subtime = int(1000*$command->{DURATION})/1000;
    $self->Dbgmsg('ListJob took ',$subtime,' seconds');
  }

  if (exists $result->{ERROR}) {
      $error .= join("\n",@{$result->{ERROR}});
  }

  # Log the monitoring command once when the job enters the queue, or on every error
  if ( $job->VERBOSE || $error ) {
    # Log the command
    my $logsafe_cmd = join(' ', @{$command->{CMD}});
    $logsafe_cmd =~ s/ -p [\S]+/ -p _censored_/;
    $job->Log($logsafe_cmd);

    # Log any extra info
    foreach ( @{$result->{INFO}} ) { chomp; $job->Log($_) };
    
    # Log any error message
    foreach ( split /\n/, $command->{STDERR} ) { chomp;  $job->Log($_) };
    
    # Only do the verbose logging once
    $job->VERBOSE(0);
  };

  foreach ( @{$job->{RAW_OUTPUT}} ) {
    if ( m%^status: getTransferJobStatus: RequestID \S+ was not found$% ) {
      $self->Alert("Job ",$job->ID." was not found. Abandoning it\n");
      $job->Timeout(-1);
    }
  }

  if ($error) { # Job monitoring failed
    $self->Alert("ListJob for ",$job->ID," returned error: $error\n");
  } else { # Job monitoring was successful
    $job->State($result->{JOB_STATE});
    $job->RawOutput(@{$result->{RAW_OUTPUT}});

    my $files = $job->Files;
    foreach ( keys %{$result->{FILES}} ) {
      my $s = $result->{FILES}{$_};
      my $f = $files->{$s->{SOURCE}};

      if ( ! $f )
      {
        $f = ASO::File->new( %{$s} );
        $job->Files($f);
      }

      if ( ! exists $f->ExitStates->{$s->{STATE}} )
      { 
        my $last = $self->{_new_file_states}{$s->{STATE}} || 0;
        if ( time - $last > 300 )
        {
          $self->{_new_file_states}{$s->{STATE}} = time;
          $self->Alert("Unknown file-state: " . $s->{STATE});
        }
      }
          
      $self->WorkStats('FILES', $f->Source, $f->State);
      $self->LinkStats($f->Source, $f->FromNode, $f->ToNode, $f->State);

      if ( $_ = $f->State( $s->{STATE} ) ) {
        $f->Log($f->Timestamp,"from $_ to ",$f->State);
        $job->Log($f->Timestamp,$f->Source,$f->Source,$f->State );
        $job->{FILE_TIMESTAMP} = $f->Timestamp;
        if ( $f->ExitStates->{$f->State} ) {
# This is a terminal state-change for the file. Log it to the Reporter
          $f->Reason($s->{REASON});
          $self->add_file_report($job->{USERNAME},$f);

          $summary = join (' ',
                           map { "$_=\"" . $s->{$_} ."\"" }
                           qw / SOURCE DESTINATION DURATION RETRIES REASON /
                          );
          $job->Log( time, 'file transfer details',$summary,"\n" );
          $f->Log  ( time, 'file transfer details',$summary,"\n" );

          foreach ( qw / DURATION RETRIES REASON / ) { $f->$_($s->{$_}); }
        }
      }
    }

    $summary = join(' ',
                    "ETC=" . $result->{ETC},
                    'JOB_STATE=' . $result->{JOB_STATE},
                    'FILE_STATES:',
                    map { $_.'='.$result->{FILE_STATES}{$_} }
                    sort keys %{$result->{FILE_STATES}}
                    );
    if ( $job->Summary ne $summary )
    {
      $self->Dbgmsg('JOBID=',$job->ID," $summary") if $self->{DEBUG};
      $job->Summary($summary);
    }

    if ( ! exists $job->ExitStates->{$result->{JOB_STATE}} )
    { 
      my $last = $self->{_new_job_states}{$result->{JOB_STATE}} || 0;
      if ( time - $last > 300 )
      {
        $self->{_new_job_states}{$result->{JOB_STATE}} = time;
        $self->Alert("Unknown job-state: " . $result->{JOB_STATE});
      }
    }

    $job->State($result->{JOB_STATE});
  }

# If the job hasn't finished in time, give up on it
  my ($job_timeout,$file_timeout,$abandon,$reason);
  $job_timeout  = $job->Timeout;
  $file_timeout = $job->FileTimeout;
  $abandon = 0;

  if ( $job_timeout && $job->Timestamp + $job_timeout < time  ) {
    $self->Alert('Abandoning JOBID=',$job->ID," after timeout ($job_timeout seconds)");
    $abandon = 1;
    $reason = "job-timeout";
  }

  if ( $file_timeout && $job->FileTimestamp && $job->FileTimestamp + $file_timeout < time  ) {
    $self->Alert('Abandoning JOBID=',$job->ID," after file state-change timeout ($file_timeout seconds)");
    $abandon = 1;
    $reason = "file-timeout";
  }

  if ( $abandon ) {
    $job->State('abandoned');

    foreach ( keys %{$job->Files} ) {
      my $f = $job->Files->{$_};
      if ( $f->ExitStates->{$f->State} == 0 ) {
        my $oldstate = $f->State('abandoned');
        $f->Log($f->Timestamp,"from $oldstate to ",$f->State);
        $f->Reason($reason);
        $self->add_file_report($job->{USERNAME},$f);
      } 
    }
  }

  $self->WorkStats('JOBS', $job->ID, $job->State);
  if ( $job->ExitStates->{$job->State} ) {
#   If the job is done/dead/abandoned, report it, but don't re-queue it.
    $kernel->yield('report_job',$job);
  } else {
# Leave priority fixed for now.
    $priority = $self->{BASE_PRIORITY};
#   $result->{ETC} = 100 if $result->{ETC} < 1;
#   $priority = $result->{ETC};
#   $priority = int($priority/60);
#   $priority = $self->{BASE_PRIORITY} if $priority < $self->{BASE_PRIORITY};
    $job->Priority($priority);
    $self->Dbgmsg('requeue JOBID=',$job->ID," Priority=",$priority) if $self->{DEBUG};
    $self->{QUEUE}->enqueue( $priority, $job );
  }

  $kernel->delay_set('poll_job', $self->{JOB_POLL_INTERVAL_FAST});
}

sub add_file_report {
  my ($self,$user,$file) = @_;
  return unless defined $self->{FN_MAP}{$file->Source};

  my $reason = $file->Reason;
  if ( $reason eq 'error during  phase: [] ' ) { $reason = ''; }

  $self->{REPORTER}{$user}{$file->Source} = {
       LFN            => delete $self->{FN_MAP}{$file->Source},
       transferStatus => $file->State,
       failure_reason => $reason,
       timestamp      => $file->Timestamp,
  };
}

sub notify_reporter {
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];
  my ($len,$totlen,$output,$user,$userdir,$reporter,$h,$f,$k,$dst);

  $totlen = 0;
  if ( defined($reporter = $self->{REPORTER}) ) {
    foreach $user ( keys %{$reporter} ) {
      undef $h;
      $len = 0;
      foreach $dst ( keys %{$reporter->{$user}} ) {
        $f = $reporter->{$user}{$dst};
        foreach $k ( qw / LFN transferStatus failure_reason timestamp / ) {
          if ( !defined($f->{$k}) ) {
            $self->Alert("File error: $k undefined for ",$dst);
          }
          push @{$h->{$k}}, $f->{$k};
        }
#       push @{$h->{LFNs}},           $_->{LFN};
#       push @{$h->{transferStatus}}, $_->{transferStatus};
#       push @{$h->{failure_reason}}, $_->{failure_reason};
#       push @{$h->{timestamp}},      $_->{timestamp};
        $len++;
      }
      $totlen += $len;
      $h->{LFNs} = delete $h->{LFN};
      $self->Logmsg("Notify Reporter of ",$len," files for $user") if $len;
      $h->{username} = $user;

      $userdir = $output = $self->{OUTBOX} . '/' . $user;
      if ( ! -d $userdir ) {
        eval {
          mkpath $userdir;
        };
        die "Cannot make directory $userdir: $@\n" if $@
      }
      $output = $userdir . '/Reporter-' . time() . '.json';
      open OUT, "> $output" or die "open $output: $!\n";
      print OUT encode_json($h);
      close OUT;
      delete $self->{REPORTER}{$user};
    }
  }

  $self->Logmsg("Notify Reporter of ",$totlen," files for all users") if $totlen;
  $kernel->delay_set('notify_reporter',$self->{REPORTER_INTERVAL});
}

sub report_job {
  my ( $self, $kernel, $job ) = @_[ OBJECT, KERNEL, ARG0 ];
  my $jobid = $job->ID;

  $self->Logmsg("JOBID=$jobid ended in state ",$job->State);
  $job->Log(time,'Job ended');
  $self->WorkStats('JOBS', $job->ID, $job->State);
  foreach ( values %{$job->Files} ) {
    $self->WorkStats('FILES', $_->Source, $_->State);
    $self->LinkStats($_->Source, $_->FromNode, $_->ToNode, $_->State);

#   Log the state-change in case it hasn't been logged already
    if ( ! $_->ExitStates->{$_->State} ) {
      $_->Reason("job-ended " . $job->State);
      $_->State('Failed');
    }
    if ( !defined($_->Timestamp) ) {
      $_->Timestamp(time);
    }
    $self->add_file_report($job->{USERNAME},$_);
  }

  $self->Dbgmsg('Log for ',$job->ID,"\n",
                $job->Log,
                "\n",'Log ends for ',$job->ID,"\n") if $self->{DEBUG};

# Now I should take detailed action on any errors...
  $self->cleanup_job_stats($job);
  $kernel->delay_set('forget_job',$self->{FORGET_JOB},$job);

# Remove the dropbox entry
  unlink $self->{WORKDIR} . '/' . $job->ID unless $self->{KEEP_INPUTS};
}

sub forget_job
{
  my ( $self, $kernel, $job ) = @_[ OBJECT, KERNEL, ARG0 ];
  delete $self->{JOBS}{$job->ID} if $job->{ID};
}

sub cleanup_job_stats
{
  my ( $self, $job ) = @_;
  my $jobid = $job->ID || 'unknown-job';
  $self->Dbgmsg("Cleaning up stats for JOBID=$jobid...") if $self->{DEBUG};
  delete $self->{WORKSTATS}{JOBS}{STATES}{$jobid};
  foreach ( values %{$job->Files} )
  {
    $self->cleanup_file_stats($_);
  }
}

sub cleanup_file_stats
{
  my ( $self, $file ) = @_;
  $self->Dbgmsg("Cleaning up stats for file source=",$file->Source) if $self->{DEBUG};
  delete $self->{WORKSTATS}{FILES}{STATES}{$file->Source};
  delete $self->{LINKSTATS}{$file->Source};
}

sub WorkStats
{
  my ($self,$class,$key,$val) = @_;
  if ( defined($class) && !defined($key))
  {
      return $self->{WORKSTATS}{$class}{STATES};
  }
  elsif ( defined($class) && defined($key) )
  {
    $self->Dbgmsg("WorkStats: class=$class key=$key value=$val") if $self->{DEBUG};
    $self->{WORKSTATS}{$class}{STATES}{$key} = $val;
    return $self->{WORKSTATS}{$class};
  }
  return $self->{WORKSTATS};
}

sub LinkStats
{
    my ($self,$file,$from,$to,$state) = @_;
    return $self->{LINKSTATS} unless defined $file &&
                                     defined $from &&
                                     defined $to;
    $self->{LINKSTATS}{$file}{$from}{$to} = $state;
    return $self->{LINKSTATS}{$file}{$from}{$to};
}

sub isKnown
{
  my ( $self, $job ) = @_;
  return 0 unless defined $self->{JOBS}{$job->ID};
  return 1;
}

sub QueueJob
{
  my ( $self, $job, $priority ) = @_;

  return if $self->isKnown($job);
  $priority = 1 unless $priority;
  $self->Logmsg('Queueing JOBID=',$job->ID,' at priority ',$priority);

  $self->WorkStats('JOBS', $job->ID, $job->State);
  foreach ( values %{$job->Files} )
  {
    $self->WorkStats('FILES', $_->Source, $_->State);
    $self->LinkStats($_->Source, $_->FromNode, $_->ToNode, $_->State);
  }
  $job->Priority($priority);
  $job->Timestamp(time);
  $self->Dbgmsg('enqueue JOBID=',$job->ID," Priority=",$priority) if $self->{DEBUG};
  $self->{QUEUE}->enqueue( $priority, $job );
  $self->{JOBS}{$job->ID} = $job;
}

1;
