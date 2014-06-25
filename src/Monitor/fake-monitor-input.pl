#!/usr/bin/perl -w
use strict;
use JSON::XS;
use Getopt::Long;
use Data::Dumper;

my ($in,$out,$json,$workflow,$payload);
my ($proxies,$lfn,$i,$h,$nFiles,$base,$copyjob,@users);
$payload = {};
GetOptions(
                'in=s'  => \$in,
                'out=s' => \$out,
          );

open IN, "<$in" or die "open input $in: $!\n";
$json = <IN>;
close IN;
$payload = decode_json($json);
$workflow = $payload->{workflow};

# Monitor input:
# {"userProxyPath":"/path/to/proxy",
#   "LFNs":["lfn1","lfn2","lfn3"],
#   "PFNs":["pfn1","pfn2","pfn3"],
#   "FTSJobid":'id-of-fts-job'}

$i = int(rand( scalar(keys %{$workflow->{proxies}}) ));
$h->{userProxyPath} = (values(%{$workflow->{proxies}}))[$i];
$h->{username}      = (keys  (%{$workflow->{proxies}}))[$i];

$base = 16*16*16*16;
$h->{FTSJobid} = sprintf("%08x-%04x-%04x-%04x-%04x%08x",
                 rand( $base * $base ),
                 rand( $base ),
                 rand( $base ),
                 rand( $base ),
                 $$,time);

# print "Proxy: $h->{userProxyPath}, FTSJobid: $h->{FTSJobid}\n";

$h->{LFNs} = [];
$h->{PFNs} = [];
$nFiles = $workflow->{nFiles};
for ( $i=0; $i<$nFiles; ++$i ) {
  $lfn = sprintf($workflow->{lfn_base},rand( $base * $base ));
  push @{$h->{LFNs}}, $lfn;
  push @{$h->{PFNs}}, 'dst:/pfn' . $lfn;
}

# Fake a copyjob
$copyjob = $workflow->{fakeFTSCacheDir} . $h->{FTSJobid};
open COPYJOB, ">$copyjob" or die "open $copyjob: $!\n";
foreach ( @{$h->{LFNs}} ) {
  print COPYJOB "src:$_ dst:/pfn$_\n";
}
close COPYJOB;

open COPYJOBSTAMP, ">$copyjob.start" or die "open $copyjob.start: $!\n";
print COPYJOBSTAMP time;
close COPYJOBSTAMP;

open  JSON, ">$workflow->{dropbox}/Monitor.$h->{FTSJobid}.json" or die "open output: $!\n";
print JSON encode_json($h);
close JSON;

#open  OUT, ">$out" or die "open output $out: $!\n";
#print OUT encode_json(\@p);
#close OUT;
exit 0;
