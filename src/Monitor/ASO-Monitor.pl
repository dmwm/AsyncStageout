#!/usr/bin/env perl

use warnings;
use strict;

##H ASO Monitor prototype, based on Utilities/fts-transfer.pl from PhEDEx
##H
##H Options:
##H 
##H --conf=$string	-> path to configuration file. The format is documented
##H   in the sample 'monitor,conf' in the github repository.
##H
##H Any 'key=value' pair that can be set in the config file can also be given
##H directly on the command line as '--key value'.
##H

# Use this for heavy debugging of POE session problems!
#sub POE::Kernel::TRACE_REFCNT () { 1 }

use Getopt::Long;
use POE;
use PHEDEX::Core::Help;
use ASO::GliteAsync;
use ASO::Monitor;

my %args = ( 'ME', 'ASOMon');

GetOptions(	"service=s"		=> \$args{service},
		"q_interval=i"		=> \$args{q_interval},
		"config=s"		=> \$args{config},
		"config_poll=i"		=> \$args{config_poll},
		"job_parallelism=i"	=> \$args{job_parallelism},
		"job_timeout=i"		=> \$args{job_timeout},
		"inbox_poll_interval=i"	=> \$args{inbox_poll_interval},
		"reporter_interval=i"	=> \$args{reporter_interval},

		"inbox=s"		=> \$args{inbox},
		"outbox=s"		=> \$args{outbox},
		"workdir=s"		=> \$args{workdir},
		"logfile=s"		=> \$args{logfile},
		"nodaemon"		=> \$args{nodaemon},
		"verbose+"		=> \$args{verbose},
		"debug+"		=> \$args{debug},
		"help"			=> \&usage,
	  );

defined($args{config}) or die "Missing argument for --config\n";
-r $args{config} or die "$args{config}: No such file or file not readable\n";

my $ASO = ASO::Monitor->new( %args );

POE::Kernel->run();
print "The POE kernel run has ended, now I shoot myself\n";
exit 0;
