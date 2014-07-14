package ASO::Job;

use strict;
use warnings;
use File::Temp qw/ tempfile tempdir /;

our %params =
	(
	  ID		=> undef,	# Determined when the job is submitted
	  SERVICE       => undef,       # FTS endpoint - backend specific!
	  TIMEOUT	=>     0,	# Timeout for total job transfer
	  PRIORITY	=>     3,	# Priority for total job transfer
	  JOB_POSTBACK	=> undef,	# Callback per job state-change
	  FILE_POSTBACK	=> undef,	# Callback per file state-change
	  FILE_TIMEOUT 	=> undef,	# Timeout for no file state-change
	  FILE_TIMESTAMP=> undef,       # Time of last file state-change
	  FILES		=> undef,	# An ASO::File array
	  SPACETOKEN	=> undef,	# A space-token for this job
	  COPYJOB	=> undef,	# Name of copyjob file
	  WORKDIR	=> undef,	# Working directory for this job
	  LOG           => undef,	# Internal log
	  RAW_OUTPUT	=> undef,       # Raw output of status command
	  SUMMARY	=> '',		# Summary of job-status so far
	  VERBOSE	=>     0,	# Verbosity for Transfer::Backend::Interface commands
	);

# These are not allowed to be set by the Autoloader...
our %ro_params =
	(
	  TIMESTAMP	=> undef,	# Time of job status reporting
	  TEMP_DIR	=> undef,	# Directory for temporary files
	  STATE		=> 'undefined',	# Initial job state
	  ME		=> undef,	# A name for this job. == ID!
	);

# See https://twiki.cern.ch/twiki/bin/view/EGEE/TransferAgentsStateMachine

our %exit_states =
	(
	  Submitted		=> 0,
	  Pending		=> 0,
	  Active		=> 0,
	  Ready			=> 0,
	  Done			=> 1,
	  DoneWithErrors	=> 0,
	  Failed		=> 1,
	  Finishing		=> 0,
	  Finished		=> 1,
	  FinishedDirty		=> 1,
	  Canceling		=> 0,
	  Canceled		=> 1,
	  Hold			=> 0,
	  undefined		=> 0,
	  lost			=> 1,
	  abandoned		=> 1,
	);

sub new {
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

  $self->{ME} = $self->{ID}; # in case it's already set...
  bless $self, $class;
  $self->Log(time,'created...');
  $self->Timestamp(time);
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

sub DESTROY
{
  my $self = shift;

  return unless $self->{COPYJOB};
# unlink $self->{COPYJOB} if -f $self->{COPYJOB};
# print $self->{ID},": Unlinked ",$self->{COPYJOB},"\n";
  $self = {};
}

sub Log
{
  my $self = shift;
  push @{$self->{LOG}}, join(' ',@_,"\n") if @_;

  return @{$self->{LOG}} if wantarray;
  return join('',@{$self->{LOG}});
}

sub RawOutput
{
  my $self = shift;
  foreach ( @_ )
  {
    chomp;
    push @{$self->{RAW_OUTPUT}}, $_ . "\n";
  }

  return @{$self->{RAW_OUTPUT}} if wantarray;
  return join('',@{$self->{RAW_OUTPUT}});
}

sub State
{
  my ($self,$state,$timestamp) = @_;
  return $self->{STATE} unless $state;
  return undef unless $self->{STATE};

  if ( $state ne $self->{STATE} )
  {
    my $oldstate = $self->{STATE};
    $self->{STATE} = $state;
    $self->{TIMESTAMP} = $timestamp || time;
    return $oldstate;
  }
  return undef;
}

sub Files
{
  my $self = shift;
  return $self->{FILES} unless @_;
  foreach ( @_ ) {
    $self->{FILES}{ $_->SOURCE } = $_;
  }
}

use Data::Dumper;
sub Dump
{
  my $self = shift;
  my ($file);
  $file = $self->{WORKDIR} . '/job-' . $self->{ID} . '.dump';

  open DUMP, "> $file" or $self->Fatal("Cannot open $file for dump of job");
  print DUMP Dumper($self);
  close DUMP;
}

sub Prepare {
die "Prepare... to die...?\n";
  my $self = shift;
  my ($fh,$file);

  if ( $file = $self->{COPYJOB} )
  {
    open FH, ">$file" or die "Cannot open file $file: $!\n";
    $fh = *FH;
  }
  else
  {
    ($fh,$file) = tempfile( undef ,
			    UNLINK => 1,
			    DIR => $self->{TEMP_DIR}
			  );
  }

# print "Using temporary file $filename\n";
  $self->{COPYJOB} = $file;
  foreach ( values %{$self->{FILES}} )
  { my $checksum_str=(defined $_->CHECKSUM_TYPE && defined $_->CHECKSUM_VAL)?
	' '.$_->CHECKSUM_TYPE.':'.$_->CHECKSUM_VAL:'';
    print $fh $_->SOURCE,' ',$_->DESTINATION,$checksum_str,"\n"; }

  close $fh;
  return $file;
}

sub ExitStates { return \%ASO::Job::exit_states; }

sub ID
{
  my $self = shift;
  $self->{ID} = $self->{ME} = shift if @_;
  return $self->{ID};
}

sub Service
{
  my $self = shift;
  $self->{SERVICE} = shift if @_;
  return $self->{SERVICE};
}

sub Timeout
{
  my $self = shift;
  $self->{TIMEOUT} = shift if @_;
  return $self->{TIMEOUT};
}

sub FileTimeout
{
  my $self = shift;
  $self->{FILE_TIMEOUT} = shift if @_;
  return $self->{FILE_TIMEOUT};
}

sub Priority
{
  my $self = shift;
  $self->{PRIORITY} = shift if @_;
  return $self->{PRIORITY};
}

sub Copyjob
{
  my $self = shift;
  $self->{COPYJOB} = shift if @_;
  return $self->{COPYJOB};
}

sub Workdir
{
  my $self = shift;
  $self->{WORKDIR} = shift if @_;
  return $self->{WORKDIR};
}

sub Summary
{
  my $self = shift;
  $self->{SUMMARY} = shift if @_;
  return $self->{SUMMARY};
}

sub Timestamp
{
  my $self = shift;
  $self->{TIMESTAMP} = shift if @_;
  return $self->{TIMESTAMP};
}

sub FileTimestamp
{
  my $self = shift;
  $self->{FILE_TIMESTAMP} = shift if @_;
  return $self->{FILE_TIMESTAMP};
}

sub Tempdir
{
  my $self = shift;
  $self->{Tempdir} = shift if @_;
  return $self->{Tempdir};
}

1;
