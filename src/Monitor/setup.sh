unset PERL5LIB
root=/data/current
. $root/sw*/slc*/cms/PHEDEX-micro/*/etc/profile.d/init.sh
. $root/sw*/slc*/cms/PHEDEX-lifecycle/*/etc/profile.d/init.sh
. $root/sw*/slc*/cms/asyncstageout/*/etc/profile.d/init.sh

# Remove old FakeFTS.pl from LifeCycle distribution!
(rm $root/sw*/slc*/cms/PHEDEX-lifecycle/*/bin/FakeFTS.pl) 2>/dev/null

# Add PHEDEX-micro Utilities to the PATH
export PATH=${PATH}:`ls -d $root/sw*/slc*/cms/PHEDEX-micro/*/Utilities`

export PERL5LIB=${PERL5LIB}:`pwd`/perl_lib
