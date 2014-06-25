package ASO::File;

use strict;
use warnings;

our %params =
	(
	  MAX_TRIES	=> 3,		# Max number of tries
	  TIMEOUT	=> 0,		# Timeout per transfer attempt
	  PRIORITY	=> 1000,	# Priority for file transfer
	  RETRY_MAX_AGE	=> 3600,	# Timeout for retrying after errors
	  LOG		=> undef,	# A Log array...
	  RETRIES	=> 0,		# Number of retries so far
	  DURATION	=> undef,	# Time taken for this transfer
	  REASON	=> undef,	# Reason for failure, if any
	  START		=> undef,	# Time this file was created
	);

# These are not allowed to be set by the Autoloader...
our %ro_params =
	(
	  SOURCE	=> undef,	# Source URL
	  DESTINATION	=> undef,	# Destination URL
	  FILESIZE      => undef,       # Filesize in TMDB
	  CHECKSUM_TYPE => undef,       # Checksum type in TMDB
 	  CHECKSUM_VAL  => undef,       # Checksum value in TMDB
	  TASKID        => undef,       # PhEDEx Task ID
 	  FROM_NODE     => undef,       # PhEDEx source node
 	  TO_NODE       => undef,       # PhEDEx destination node
	  WORKDIR       => undef,       # workdir of a job(!)         
	  TIMESTAMP	=> undef,	# Time of file status reporting
	  STATE		=> 'undefined',	# Initial file state
	  ME		=> undef,	# A name for this job. == TASKID!
	);

# See https://twiki.cern.ch/twiki/bin/view/EGEE/TransferAgentsStateMachine

our %exit_states =
	(
	  Submitted	=> 0,
	  Pending	=> 0,
	  Ready		=> 0,
	  Active	=> 0,
	  Done		=> 1,
	  Waiting	=> 0,
	  Hold		=> 0,
	  Failed	=> 2,
	  Finishing	=> 0,
	  Finished	=> 1,
	  AwaitingPrestage => 0,
	  Prestaging => 0,
	  WaitingPrestage => 0,
	  WaitingCatalogResolution => 0,
	  WaitingCatalogRegistration => 0,
	  Canceled	=> 2,
	  undefined	=> 0,
	  lost		=> 2,
	  abandoned     => 2,
	);

sub new
{
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ref($proto) ? $class->SUPER::new(@_) : {};

  my %args = (@_);
  map {
        $self->{$_} = defined($args{$_}) ? delete $args{$_} : $params{$_}
      } keys %params;
  map {
        $self->{$_} = defined($args{$_}) ? delete $args{$_} : $ro_params{$_}
      } keys %ro_params;
  map { $self->{$_} = $args{$_} } keys %args;
  $self->{LOG} = [];
  $self->{ME} = $self->{TASKID}; # in case it's already set

  bless $self, $class;
  return $self;
}

sub AUTOLOAD
{
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;

  return $self->{$attr} if exists $ro_params{$attr};

  if ( exists($params{$attr}) )
  {
    $self->{$attr} = shift if @_;
    return $self->{$attr};
  }

  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  my $parent = "SUPER::" . $attr;
  $self->$parent(@_);
}

sub Log
{
  my $self = shift;
  push @{$self->{LOG}}, join(' ',@_,"\n") if @_;

# return undef unless defined $self->{LOG};
  return @{$self->{LOG}} if wantarray;
  return join('',@{$self->{LOG}});
}

sub State
{
  my ($self,$state,$time) = @_;
  return $self->{STATE} unless $state;
  return undef if ( $state eq $self->{STATE} );

  my $oldstate = $self->{STATE};
  $self->{STATE} = $state;
  $self->{TIMESTAMP} = $time || time;
  return $oldstate;
}

sub ExitStates { return \%ASO::File::exit_states; }

sub Retry
{
  my $self = shift;
  $self->{RETRIES}++;
  return 0 if $self->{RETRIES} >= $self->{MAX_TRIES};
  undef $self->{STATE};
  $self->Log(time,'reset for retry');
  return $self->{RETRIES};
}

sub Retries
{
  my $self = shift;
  return $self->{RETRIES};
}

sub Nice
{
  my $self = shift;
  my $nice = shift || -4;
  $self->{PRIORITY} += $nice;
}

sub WriteLog
{
  my $self = shift;
  my $dir = shift;
  return unless $dir;

  my ($fh,$logfile);
  $logfile = $self->{DESTINATION};
  $logfile =~ s%^.*/store%%;
  $logfile =~ s%^.*=%%;
  $logfile =~ s%\/%_%g;
  $logfile = $dir . '/file-' . $logfile . '.log';
  open $fh, ">$logfile" || die "open: $logfile: $!\n";
  print $fh scalar localtime time, ' Log for ',$self->{DESTINATION},"\n",
            $self->Log,
            scalar localtime time," Log ends\n";
  close $fh;
}

sub Timeout
{
  my $self = shift;
  $self->{TIMEOUT} = shift if @_;
  return $self->{TIMEOUT};
}

sub Priority
{
  my $self = shift;
  $self->{PRIORITY} = shift if @_;
  return $self->{PRIORITY};
}

sub MaxTries
{
  my $self = shift;
  $self->{MAX_TRIES} = shift if @_;
  return $self->{MAX_TRIES};
}

sub Source
{
  my $self = shift;
  $self->{SOURCE} = shift if @_;
  return $self->{SOURCE};
}

sub ChecksumType
{
  my $self = shift;
  $self->{CHECKSUM_TYPE} = shift if @_;
  return $self->{CHECKSUM_TYPE};
}

sub ChecksumValue
{
  my $self = shift;
  $self->{CHECKSUM_VAL} = shift if @_;
  return $self->{CHECKSUM_VAL};
}

sub Destination
{
  my $self = shift;
  $self->{DESTINATION} = shift if @_;
  return $self->{DESTINATION};   
}

sub TaskID
{
  my $self = shift;
  $self->{TASKID} = shift if @_;
  $self->{ME} = $self->{TASKID};
  return $self->{TASKID};
}

sub FromNode
{
  my $self = shift;
  $self->{FROM_NODE} = shift if @_;
  return $self->{FROM_NODE};
}

sub ToNode
{
  my $self = shift;
  $self->{TO_NODE} = shift if @_;
  return $self->{TO_NODE};
}

sub Workdir
{
  my $self = shift;
  $self->{WORKDIR} = shift if @_;
  return $self->{WORKDIR};
}

sub Timestamp
{
  my $self = shift;
  $self->{TIMESTAMP} = shift if @_;
  return $self->{TIMESTAMP};
}

sub Reason
{
  my $self = shift;
  $self->{REASON} = shift if @_;
  return $self->{REASON};
}

sub Duration
{
  my $self = shift;
  $self->{DURATION} = shift if @_;
  return $self->{DURATION};
}

sub Start
{
  my $self = shift;
  $self->{START} = shift if @_;
  return $self->{START};
}

sub RetryMaxAge
{
  my $self = shift;
  $self->{RETRY_MAX_AGE} = shift if @_;
  return $self->{RETRY_MAX_AGE};
}

1;
