package ASO::Glite;

use strict;
use warnings;
use base 'PHEDEX::Core::Logging';

our %params =
	(
	  SERVICE	=> undef,	# Transfer service URL
	  SPACETOKEN	=> undef,       
	  CHANNEL	=> undef,	# Channel name to match
	  USERDN	=> undef,	# Restrict to specific user DN
	  VONAME	=> undef,	# Restrict to specific VO
	  SSITE		=> undef,	# Specify source site name
	  DSITE		=> undef,	# Specify destination site name
	  ME		=> 'Glite',	# Arbitrary name for this object
	  PRIORITY	=> 3,		# Default piority configured in FTS channels
	);

our %states =
	(
	  Submitted		=> 11,
	  Ready			=> 10,
	  Active		=>  9,
	  Finished		=>  0,
	  FinishedDirty		=>  0,
	  Pending		=> 10,
	  Default		=> 99,
	);

our %weights =
	(
	  Ready	  =>  1 + 0 * 300,
	  Active  =>  1 + 0 * 300,
	  Waiting =>  1 + 0 * 900,
	);

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ref($proto) ? $class->SUPER::new(@_) : {};

  my %args = (@_);
  map { 
        $self->{$_} = defined($args{$_}) ? $args{$_} : $params{$_}
      } keys %params;
  $self->{DEBUGGING} = $PHEDEX::Debug::Paranoid || 0;

  bless $self, $class;
  return $self;
}

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  if ( exists($params{$attr}) )
  {
    $self->{$attr} = shift if @_;
    return $self->{$attr};
  }
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  my $parent = "SUPER::" . $attr;
  $self->$parent(@_);
}

sub ListQueue {
  my $self = shift;
  my ($cmd,$job,$state,%result,@raw);

  $result{JOBS} = {};
  $cmd = "glite-transfer-list -s " . $self->{SERVICE};
  open GLITE, "$cmd 2>&1 |" or do
  {
      print $self->Hdr,"$cmd: $!\n";
      $result{ERROR} = 'ListQueue: ' . $self->{SERVICE} . ': ' . $!;
      return \%result;
  };
  while ( <GLITE> )
  {
    push @raw, $_;
    m%^([0-9,a-f,-]+)\s+(\S+)$% or next;
    $result{JOBS}{$1} = { ID => $1, STATE => $2, SERVICE => $self->{SERVICE} };
  }
  close GLITE or do
  {
      print $self->Hdr,"close: $cmd: $!\n";
      $result{ERROR} = 'close ListQueue: ' . $self->{SERVICE} . ': ' . $!;
  };
  $result{RAW_OUTPUT} = \@raw;
  return \%result;
}

sub ListJob {
  my ($self,$job) = @_;
  my ($cmd,$state,%result,$dst,@raw);
  my ($key,$value);
  my (@h,$h,$preamble);

  $cmd = 'glite-transfer-status -l ';
  $cmd .= ' --verbose' if $job->VERBOSE;
  $cmd .= ' -s ' . $job->Service . ' ' . $job->ID;
  open GLITE, "$cmd 2>&1 |" or do
  {
      print $self->Hdr,"$cmd: $!\n";
      $result{ERROR} = 'ListJob: ' . $job->ID . ': ' . $!;
      return \%result;
  };
  @raw = <GLITE>;
  $result{RAW_OUTPUT} = \@raw;
  close GLITE or do
  {
      print $self->Hdr,"close: $cmd: $!\n@raw";
      $result{ERROR} = 'close ListJob: ' . $job->ID . ':' . $!;
      return \%result;
  };

  $preamble=1;
  my $last_key;

  while ( $_ = shift @raw )
  {
    if ( $preamble )
    {
      if ( m%^\s*([A-Z,a-z]+)\s*$% ) # non-verbose case
      {
        $state = $1;
        $preamble = 0;
      }
      if ( m%^\s*Status:\s+([A-Z,a-z]+)\s*$% ) # verbose case
      {
        $state = $1;
      }
      if ( m%^\s+Source:\s+(.*)\s*$% )
      {
        unshift @raw, $_;
        $preamble = 0;
      }
      push @{$result{INFO}}, $_ if $preamble;
      next;
    }

    if ( m%^\s+Source:\s+(.*)\s*$% )
    {
#     A 'Source' line is the first in a group for a single src->dst transfer
      push @h, $h if $h;
      undef $h;
    }
    if ( m%^\s+(\S+):\s+(.*)\s*$% )
    {
      $last_key = uc $1;
      $h->{$last_key} = $2;
    }
    elsif ( m%\S% )
    {
      $h->{$last_key} .= ' ' . $_;
    }
  }

  chomp $state if (defined $state);
  $result{JOB_STATE} = $state || 'undefined';

  push @h, $h if $h;
  foreach $h ( @h )
  {
#  Be paranoid about the fields I read!
    foreach ( qw / DESTINATION DURATION REASON RETRIES SOURCE STATE / ) {
      die "No \"$_\" key! : ", map { "$_=$h->{$_} " } sort keys %{$h}
        unless defined($h->{$_});
    }
    $result{FILES}{$h->{DESTINATION}} = $h;
  }

  my $time = time;
  foreach ( keys %{$result{FILES}} )
  {
    $result{FILE_STATES}{ $result{FILES}{$_}{STATE} }++;
    $result{FILES}{$_}{TIMESTAMP} = $time;
  }

  $result{ETC} = 0;
# foreach ( keys %{$result{FILE_STATES}} )
# {
#   $result{ETC} += ( $weights{$_} || 0 ) * $result{FILE_STATES}{$_};
# }

  return \%result;
}

sub StatePriority
{
  my ($self,$state) = @_;
  return $states{$state} if defined($states{$state});
  return $states{Default} if !$self->{DEBUGGING};
  die "Unknown state \"$state\" encountered in ",__PACKAGE__,"\n";
}

sub SetPriority
{
  my ($self,$job) = @_;
  my ($priority,@raw,%result);
  return unless $priority = $job->Priority;
  return if $priority == $self->{PRIORITY}; # Save an interaction with the server

  my $cmd = "glite-transfer-setpriority";
  if ( $job->Service ) { $cmd .= ' -s ' . $job->Service; }
  $cmd .= ' ' . $job->ID . ' ' . $priority;
  print $self->Hdr,"Execute: $cmd\n";
  $result{CMD} = $cmd;
  open GLITE, "$cmd 2>&1 |" or die "$cmd: $!\n";
  while ( <GLITE> )
  {
    push @raw, $_;
  }
  close GLITE or do
  {
      print $self->Hdr,"close: $cmd: $!\n";
      $result{ERROR} = 'close SetPriority: id=' . $job->ID . ' ' . $!;
  };
  $result{RAW_OUTPUT} = \@raw;
  return \%result;
}

sub Submit
{
  my ($self,$job) = @_;
  my (%result,@raw,$id);

  defined $job->COPYJOB or do
  {
    $result{ERROR} = 'Submit: No copyjob given for job ' . $job->ID;
    return \%result;
  };

  my $cmd = "glite-transfer-submit". 
      ' -s ' . $job->Service .
      ((defined $self->SPACETOKEN) ? ' -t '.$self->SPACETOKEN : "") .
      ' -f ' . $job->Copyjob;

  my $logsafe_cmd = $cmd;
  $logsafe_cmd =~ s/ -p [\S]+/ -p _censored_/;
  push @{$result{INFO}}, $logsafe_cmd . "\n";

  open GLITE, "$cmd 2>&1 |" or die "$logsafe_cmd: $!\n";
  while ( <GLITE> )
  {
    push @raw, $_;
    chomp;
    m%^([0-9,a-f,-]+)\s*$% or next;
    $id = $_ unless $id;
  }
  $result{RAW_OUTPUT} = \@raw;
  close GLITE or do
  {
      print $self->Hdr,"close: $logsafe_cmd: $!\n";
      $result{ERROR} = 'close Submit: JOBID=' . ( $id || 'undefined' ) . $!;
      return \%result;
  };
  print $self->Hdr,"JOBID=$id submitted...\n";
  $result{ID} = $id;
  $job->ID($id);

  $result{SETPRIORITY} = $self->SetPriority($job);

  return \%result;
}

1;
