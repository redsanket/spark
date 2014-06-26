#!/bin/sh

cat &> /dev/null

echo "Permissions of testDir/cppExecutables" 1>&2
ls -lR ./cppExecutables 1>&2
ls -lHR ./cppExecutables 1>&2

echo "Permissions of testDir/perlExecutables" 1>&2
ls -lR ./perlExecutables 1>&2
ls -lHR ./perlExecutables 1>&2

echo "Permissions of testDir/shellExecutables" 1>&2
ls -lR ./shellExecutables 1>&2
ls -lHR ./shellExecutables 1>&2

echo "Permissions of testDir/allExecutables" 1>&2
ls -lR ./allExecutables 1>&2
ls -lHR ./allExecutables 1>&2

echo "Permissions of testDir/allExecutables/perlExecutables/" 1>&2
ls -lR ./allExecutables/perlExecutables/ls -lHR ./allExecutables/perlExecutables/ 1>&2

echo "Permissions of testDirallExecutables/subDir1/shellExecutables/" 1>&2
ls -lR ./allExecutables/subDir1/shellExecutables/ 1>&2
ls -lRH ./allExecutables/subDir1/shellExecutables/ 1>&2

echo "Permissions of testDir/allExecutables/subDir3/subDir31/perlExecutables/" 1>&2
ls -lR ./allExecutables/subDir3/subDir31/perlExecutables/ 1>&2
ls -lRH ./allExecutables/subDir3/subDir31/perlExecutables/ 1>&2

echo "Permissions of testDirallExecutables/subDir2/cppExecutables/" 1>&2
ls -lR ./allExecutables/subDir2/cppExecutables/ls -lRH ./allExecutables/subDir2/cppExecutables/ 1>&2

./cppExecutables/cpp1.out
./cppExecutables/cpp2.out
./cppExecutables/cpp3.out
./cppExecutables/cpp4.out
./cppExecutables/cpp5.out
./cppExecutables/cpp6.out
./cppExecutables/cpp7.out

./perlExecutables/perl1.pl
./perlExecutables/perl2.pl
./perlExecutables/perl3.pl
./perlExecutables/perl4.pl
./perlExecutables/perl5.pl
./perlExecutables/perl6.pl
./perlExecutables/perl7.pl

./shellExecutables/shell1.sh
./shellExecutables/shell2.sh
./shellExecutables/shell3.sh
./shellExecutables/shell4.sh
./shellExecutables/shell5.sh
./shellExecutables/shell6.sh
./shellExecutables/shell7.sh

./allExecutables/perlExecutables/perl1.pl
./allExecutables/perlExecutables/perl2.pl
./allExecutables/perlExecutables/perl3.pl
./allExecutables/perlExecutables/perl4.pl
./allExecutables/perlExecutables/perl5.pl
./allExecutables/perlExecutables/perl6.pl
./allExecutables/perlExecutables/perl7.pl

./allExecutables/subDir1/shellExecutables/shell1.sh
./allExecutables/subDir1/shellExecutables/shell2.sh
./allExecutables/subDir1/shellExecutables/shell3.sh
./allExecutables/subDir1/shellExecutables/shell4.sh
./allExecutables/subDir1/shellExecutables/shell5.sh
./allExecutables/subDir1/shellExecutables/shell6.sh
./allExecutables/subDir1/shellExecutables/shell7.sh

./allExecutables/subDir3/subDir31/perlExecutables/perl1.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl2.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl3.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl4.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl5.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl6.pl
./allExecutables/subDir3/subDir31/perlExecutables/perl7.pl

./allExecutables/subDir2/cppExecutables/cpp1.out
./allExecutables/subDir2/cppExecutables/cpp2.out
./allExecutables/subDir2/cppExecutables/cpp3.out
./allExecutables/subDir2/cppExecutables/cpp4.out
./allExecutables/subDir2/cppExecutables/cpp5.out
./allExecutables/subDir2/cppExecutables/cpp6.out
./allExecutables/subDir2/cppExecutables/cpp7.out

