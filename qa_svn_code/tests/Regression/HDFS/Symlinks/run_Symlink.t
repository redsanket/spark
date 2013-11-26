#!/usr/local/bin/perl

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 45;
use Hadoop::FS;
use List::MoreUtils qw(uniq);

# This test plan addresses HADOOP-6421
# http://twiki.corp.yahoo.com/pub/Grid/HdfsCommonTestPlan_0_22_0/SymlinksTestPlan.html

my $self = __PACKAGE__->new;
my $Config = $self->config;
my $fs = new Hadoop::FS($self);

# Test framework specific varibles
$ENV{OWNER} = 'jeagles';

# convenience varibles to make the tests better match the test cases in the test plan
# currently there is only one namenode
my $nn1 = $Config->{NODES}->{NAMENODE}->{HOST1} . ':' . $Config->{NODES}->{NAMENODE}->{PORT};
my $nn2 = $Config->{NODES}->{NAMENODE}->{HOST1} . ':' . $Config->{NODES}->{NAMENODE}->{PORT};
my $userName = $ENV{USER};

# Predeclare these widely used variables
my ($stdout, $stderr, $success, $exit_code);

my $nn1_test_dir = "hdfs://$nn1/user/$userName";
my $nn2_test_dir = "hdfs://$nn2/user/$userName";

my $dirs = [
# uniq accounts for there only being one namenode currently.
uniq(
	"hdfs://$nn1/user/$userName/level1",
	"hdfs://$nn1/user/$userName/inputdir",
	"hdfs://$nn1/user/$userName/inputdir/level2",
	"hdfs://$nn1/user/$userName/outputdir",
	"hdfs://$nn1/user/$userName/outputdir/level2",
	"hdfs://$nn1/user/$userName/symlinkdir",
	"hdfs://$nn2/user/$userName/inputdir",
	"hdfs://$nn2/user/$userName/inputdir/level2",
	"hdfs://$nn2/user/$userName/outputdir",
	"hdfs://$nn2/user/$userName/outputdir/level2",
	"hdfs://$nn2/user/$userName/symlinkdir",
)
];

# a large text file that would be suitable for wordcount.
my $files = {
	"$Config->{HADOOP_COMMON_HOME}/README.txt" => "hdfs://$nn1/user/$userName/inputdir/infile01",
	"$Config->{HADOOP_COMMON_HOME}/LICENSE.txt" => "hdfs://$nn1/user/$userName/inputdir/level2/infile02",
};

my $symlinks = { 
	"hdfs://$nn1/user/$userName/symlinkdir/input1" => "hdfs://$nn1/user/$userName/inputdir",
	"hdfs://$nn1/user/$userName/symlinkdir/output1" => "hdfs://$nn1/user/$userName/outputdir",
	"hdfs://$nn1/user/$userName/symlinkdir/myuser1" => "hdfs://$nn1/user/$userName",
	"hdfs://$nn2/user/$userName/symlinkdir/input2" => "hdfs://$nn2/user/$userName/inputdir",
	"hdfs://$nn2/user/$userName/symlinkdir/output2" => "hdfs://$nn2/user/$userName/outputdir",
	"hdfs://$nn2/user/$userName/symlinkdir/myuser2" => "hdfs://$nn2/user/$userName",
};

sub verify_fs_command {
	($stdout, $stderr, $success, $exit_code) = @_;
	if ($exit_code) {
		note('OUT:' . $stdout);
		note('ERR:' . $stderr);
		note('SUCCESS:' . $success);
		note('EXIT CODE:' . $exit_code);
	}

	return @_;
}

# should only be executed once
sub init_test_plan {

	my $testfile_cmd = "cp $Config->{HADOOP_COMMON_HOME}/LICENSE.txt ~/testfile.txt";
	note($testfile_cmd);
	system($testfile_cmd) == 0
		or diag("$testfile_cmd Failed");;
}

# should be executed at the beginning of each test
sub init_hdfs {

	# make sure we have a clear user directory
	# The trash by default is in the home directory
	verify_fs_command($fs->rmr("-skipTrash", $nn1_test_dir));
	verify_fs_command($fs->mkdir($nn1_test_dir));
	verify_fs_command($fs->test('-d', $nn1_test_dir));

	verify_fs_command($fs->rmr("-skipTrash", $nn2_test_dir));
	verify_fs_command($fs->mkdir($nn2_test_dir));
	verify_fs_command($fs->test('-d', $nn2_test_dir));

	# first make dirs
	# sort ensures depth first search directory creation
	for my $dir (sort @$dirs) {
		verify_fs_command($fs->mkdir($dir));
		verify_fs_command($fs->test('-d', $dir));
	}

	# next make files
	while (my ($src, $dst) = each %$files) {
		verify_fs_command($fs->copyFromLocal($src, $dst));
		verify_fs_command($fs->test('-e', $dst));
	}

	# finally make symlinks
	while (my ($symlink, $target) = each %$symlinks) {
		verify_fs_command($fs->ln($target, $symlink));
	}
}

# should be executed at the end of each test
sub cleanup_hdfs {

	# first remove symlinks
	for my $symlink (keys %$symlinks) {
		verify_fs_command($fs->rm($symlink));
	}

	# next remove files
	for my $file (values %$files) {
		verify_fs_command($fs->rm($file));
	}

	# finally remove directories
	# sort ensures depth first search directory removal
	for my $dir (reverse sort @$dirs) {
		verify_fs_command($fs->rmr($dir));
	}
}

# put file into real dir and verify it shows up in the linked dir
sub Symlinks_01 {

	verify_fs_command($fs->put('~/testfile.txt', "hdfs://$nn1/user/$userName"));
	(undef, $stderr, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/symlinkdir/myuser1/testfile.txt");
	is($exit_code, 0, "Symlinks_01: Can't find test file in link directory: $stderr: Blocked by Bug 4423401");
}

# put file into symlink dir and verify it shows up in the linked dir
sub Symlinks_02 {

	verify_fs_command($fs->put("~/testfile.txt",  "hdfs://$nn1/user/$userName/symlinkdir/myuser1/level1"));
	(undef, $stderr, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/symlinkdir/myuser1/level1/testfile.txt");
	is($exit_code, 0, "Symlinks_02: Can't find test file in link directory: $stderr: Blocked by Bug 4423401");
}

# put file into symlink dir and verify it shows up in the real dir
sub Symlinks_03 {

	verify_fs_command($fs->put("~/testfile.txt", "hdfs://$nn1/user/$userName/symlinkdir/myuser1"));
	(undef, $stderr, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/testfile.txt");
	is($exit_code, 0, "Symlinks_03: Can't find test file in link directory: $stderr: Blocked by Bug 4423401");
}

# put file into symlink dir and verify contents of file in the symlink dir
sub Symlinks_04 {

	verify_fs_command($fs->put("~/testfile.txt", "hdfs://$nn1/user/$userName/symlinkdir/myuser1"));
	($stdout, $stderr) = verify_fs_command($fs->md5("hdfs://$nn1/user/$userName/symlinkdir/myuser1/testfile.txt"));
	chomp($stdout);
	my $md5 = `md5sum ~/testfile.txt`;
	chomp($md5);
	is($stdout, $md5, "Symlinks_04: Md5 sum of file mismatch: $stderr: Blocked by Bug 4423401");
}

sub Symlinks_05 {

	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/infile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/testFileSym06"));
	my ($stdout1, $stderr1, undef, $exit_code1) = verify_fs_command($fs->md5("hdfs://$nn1/user/$userName/symlinkdir/input1/infile01"));
	my ($stdout2, $stderr2, undef, $exit_code2) = verify_fs_command($fs->md5("hdfs://$nn1/user/$userName/symlinkdir/output1/testFileSym06"));
	chomp($stdout1);
	chomp($stdout2);
	my $ok = (($exit_code1 == 0) && ($exit_code2 == 0) && ($stdout1 ne '') && ($stdout1 eq $stdout2));
	ok($ok, "Symlinks_05: Md5 sum of file mismatch: $stderr1: $stderr2: Blocked by Bug 4423401");
}

sub Symlinks_06 {

	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/infile01", "hdfs://$nn2/user/$userName/symlinkdir/output2/testFileSym07"));
	my ($stdout1, $stderr1, undef, $exit_code1) = verify_fs_command($fs->md5("hdfs://$nn1/user/$userName/symlinkdir/input1/infile01"));
	my ($stdout2, $stderr2, undef, $exit_code2) = verify_fs_command($fs->md5("hdfs://$nn2/user/$userName/symlinkdir/output2/testFileSym07"));
	chomp($stdout1);
	chomp($stdout2);
	my $ok = (($exit_code1 == 0) && ($exit_code2 == 0) && ($stdout1 ne '') && ($stdout1 eq $stdout2));
	ok($ok, "Symlinks_06: Md5 sum of file mismatch: $stderr1: $stderr2: Blocked by Bug 4423401");
}

sub Symlinks_07 {

	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/level2/infile02", "hdfs://$nn2/user/$userName/symlinkdir/output2/level1/testFileSym08"));
	my ($stdout1, $stderr1, undef, $exit_code1) = verify_fs_command($fs->md5("hdfs://$nn1/user/$userName/symlinkdir/input1/level2/infile02"));
	my ($stdout2, $stderr2, undef, $exit_code2) = verify_fs_command($fs->md5("hdfs://$nn2/user/$userName/symlinkdir/output2/level1/testFileSym08"));
	chomp($stdout1);
	chomp($stdout2);
	my $ok = (($exit_code1 == 0) && ($exit_code2 == 0) && ($stdout1 ne '') && ($stdout1 eq $stdout2));
	ok($ok, "Symlinks_07: Md5 sum of file mismatch: $stderr1: $stderr2: Blocked by Bug 4423401");
}

sub Symlinks_08 {

	my $src = "hdfs://$nn1/user/$userName/symlinkdir/input1";
	my $dst = "hdfs://$nn1/user/$userName/symlinkdir/output1";
	verify_fs_command($fs->cp($src, $dst));

	my @test_dirs = map { if (m/$symlinks->{$src}(.*)/) {$1} else {()} } @$dirs;
	for my $dir (@test_dirs) {
		(undef, $stderr, undef, $exit_code) = verify_fs_command($fs->test('-d', $dst . $dir));
		is($exit_code, 0, "Symlinks_08: Can't find test dir in recursive copy of link directory: $stderr: Blocked by Bug 4423401");
	}
	my @test_files = map { if (m/$symlinks->{$src}(.*)/) {$1} else {()} } values %$files;
	for my $file (@test_files) {
		(undef, $stderr, undef, $exit_code) = verify_fs_command($fs->test('-e', $dst . $file));
		is($exit_code, 0, "Symlinks_08: Can't find test file in recursive copy of link directory: $stderr: Blocked by Bug 4423401");
	}
}

sub Symlinks_09 {

	(undef, $stderr, undef, $exit_code) = verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/myuser1", "hdfs://$nn1/user/$userName/symlinkdir/output1"));
	isnt($exit_code, 0, "Symlinks_09: Shouldn't be able to copy a parent directory into a child directory: Blocked by Bug 4423401");
}

sub Symlinks_10 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/user/$userName/symlinkdir/input1/infile01";
	my $outputdir = "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT10";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_10: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_11 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/user/$userName/symlinkdir/input1/infile01 ";
	my $outputdir = "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT11";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_11: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_12 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/inputdir/infile01";
	my $outputdir = "hdfs://$nn1/outputdir/testresultsCSMT12";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_12: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_13 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/user/$userName/symlinkdir/input1/infile01";
	my $outputdir = "hdfs://$nn1/outputdir/testresultsCSMT13";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_13: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_14 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/inputdir/infile01";
	my $outputdir = "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT14";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_14: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_15 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/user/$userName/symlinkdir/input1/infile01";
	my $outputdir = "hdfs://$nn2/user/$userName/symlinkdir/output2/testresultsCSMT15";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_15: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_16 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn2/inputdir/infile01";
	my $outputdir = "hdfs://$nn1/outputdir/testresultsCSMT16";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_16: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_17 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn2/user/$userName/symlinkdir/input2/infile01";
	my $outputdir = "hdfs://$nn1/outputdir/testresultsCSMT17";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_17: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_18 {

	my $control_inputfile = "hdfs://$nn1/user/$userName/inputdir/infile01";
	my $control_outputdir = "hdfs://$nn1/user/$userName/outputdir/control_testresults";
	verify_fs_command($self->run_wordcount_job({args => [$control_inputfile, $control_outputdir]}));
	my ($control_stdout, undef, undef, $control_exit_code) = verify_fs_command($fs->cat("$control_outputdir/*"));

	my $inputfile = "hdfs://$nn1/inputdir/infile01";
	my $outputdir = "hdfs://$nn2/user/$userName/symlinkdir/output2/testresultsCSMT18";
	verify_fs_command($self->run_wordcount_job({args => [$inputfile, $outputdir]}));
	($stdout, undef, undef, $exit_code) = verify_fs_command($fs->cat("$outputdir/*"));

	my $ok = ($control_exit_code == 0) && ($exit_code == 0) && ($control_stdout eq $stdout);
	ok($ok, "Symlinks_18: wordcount output does not match expected result: Blocked by Bug 4423401");
}

sub Symlinks_19 {

	(undef, undef, undef, $exit_code) = $fs->test('-d', "hdfs://$nn1/user/$userName/.Trash");
	isnt($exit_code, 0, "Symlinks_19: .Trash directory should not exist yet");
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT19"));
	verify_fs_command($fs->rm("hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT19"));
	(undef, $stderr, undef, $exit_code) = $fs->test('-d', "hdfs://$nn1/user/$userName/.Trash/");
	is($exit_code, 0, "Symlinks_19: .Trash should now exist: Blocked by Bug 4423401: $stderr");
	(undef, $stderr, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/.Trash/Current/user/$userName/outputdir/testresultsCSMT19");
	is($exit_code, 0, "Symlinks_19: Removed file should now exist in the .Trash directory: Blocked by Bug 4423401: $stderr");
}

sub Symlinks_20 {

	(undef, undef, undef, $exit_code) = verify_fs_command($fs->touchz("hdfs://$nn1/user/$userName/tmpfile"));
	is($exit_code, 0, "Symlinks_20: Touched temp file in home directory");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->rm("hdfs://$nn1/user/$userName/tmpfile"));
	is($exit_code, 0, "Symlinks_20: Removed temp file in home directory");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	is($exit_code, 0, "Symlinks_20: .Trash should now exist");
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT20"));
	$fs->rm("hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT20");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-e', "hdfs://$nn1/user/$userName/.Trash/Current/user/$userName/outputdir/testresultsCSMT20"));
	is($exit_code, 0, "Symlinks_20: Removed file should now exist in the .Trash directory: Blocked by Bug 4423401");
}

sub Symlinks_21 {

	(undef, undef, undef, $exit_code) = $fs->test('-d', "hdfs://$nn1/user/$userName/.Trash");
	isnt($exit_code, 0, "Symlinks_21: .Trash directory should not exist yet");
	verify_fs_command($fs->mkdir("hdfs://$nn1/user/$userName/symlinkdir/output1/level2/level3/"));
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/level2/level3/testresultsCSMT21"));
	verify_fs_command($fs->rmr("hdfs://$nn1/user/$userName/symlinkdir/output1/level3"));
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	is($exit_code, 0, "Symlinks_21: .Trash directory should now exist: Blocked by Bug 4423401");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-e', "hdfs://$nn1/user/$userName/.Trash/Current/user/$userName/outputdir/level2/level3/testresultsCSMT21"));
	is($exit_code, 0, "Symlinks_21: Test results file should exist in the .Trash directory: Blocked by Bug 4423401");
}

sub Symlinks_22 {

	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	isnt($exit_code, 0, "Symlinks_22: .Trash directory should not exist");
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT22"));
	verify_fs_command($fs->rm("-skipTrash", "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT22"));
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-e', "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT22"));
	isnt($exit_code, 0, "Symlinks_22: Removed file should no longer exist: Blocked by Bug 4423401");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	isnt($exit_code, 0, "Symlinks_22: .Trash directory should not exist due to skipTrash option: Blocked by Bug 4423401");
}

# if the trash already exists, don't put the removed files into the already existing trash if skipTrash is specified
sub Symlinks_23 {

	(undef, undef, undef, $exit_code) = verify_fs_command($fs->touchz("hdfs://$nn1/user/$userName/tmpfile"));
	is($exit_code, 0, "Symlinks_23: Touched temp file in home directory");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->rm("hdfs://$nn1/user/$userName/tmpfile"));
	is($exit_code, 0, "Symlinks_23: Removed temp file in home directory");
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	is($exit_code, 0, "Symlinks_23: .Trash should now exist");
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT23"));
	verify_fs_command($fs->rm('-skipTrash', "hdfs://$nn1/user/$userName/symlinkdir/output1/testresultsCSMT23"));
	(undef, undef, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/.Trash/Current/user/$userName/outputdir/testresultsCSMT23");
	isnt($exit_code, 0, "Symlinks_23: File removed with skipTrash should not exist in the .Trash directory: Blocked by Bug 4423401");
}

sub Symlinks_24 {

	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	isnt($exit_code, 0, "Symlinks_24: .Trash directory should not exist");
	verify_fs_command($fs->mkdir("hdfs://$nn1/user/$userName/symlinkdir/output1/level2/level3/"));
	verify_fs_command($fs->cp("hdfs://$nn1/user/$userName/symlinkdir/input1/testfile01", "hdfs://$nn1/user/$userName/symlinkdir/output1/level2/level3/testresultsCSMT24"));
	verify_fs_command($fs->rmr('-skipTrash', "hdfs://$nn1/user/$userName/symlinkdir/output1/level3"));
	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/.Trash"));
	isnt($exit_code, 0, "Symlinks_24: .Trash directory should not exist due to skipTrash option: Blocked by Bug 4423401");
}

sub Symlinks_25 {

	(undef, undef, undef, $exit_code) = $fs->test('-e', "hdfs://$nn1/user/$userName/badfile/");
	isnt($exit_code, 0, "Symlinks_25: Bad file should not exist");
	(undef, undef, undef, $exit_code) = $fs->ln("hdfs://$nn1/user/$userName/symlinkdir/badlink", "hdfs://$nn1/user/$userName/badfile/");
	isnt($exit_code, 0, "Symlinks_25: Should not be able to create a symlink to a non-existent file: Blocked by Bug 4423401");
}

sub Symlinks_26 {

	(undef, undef, undef, $exit_code) = verify_fs_command($fs->test('-d', "hdfs://$nn1/user/$userName/outputdir/"));
	is($exit_code, 0, "Symlinks_26: Directory should already exist");
	(undef, undef, undef, $exit_code) = $fs->ln("hdfs://$nn1/user/$userName/symlinkdir/badlink", "hdfs://$nn1/user/$userName/symlinkdir/badlink");
	isnt($exit_code, 0, "Symlinks_26: Should not be able to create a symlink to itself: Blocked by Bug 4423401");
}

sub Symlinks_27 {

	verify_fs_command($fs->mkdir("hdfs://$nn1/user/$userName/noaccess/"));
	verify_fs_command($fs->chmod('000', "hdfs://$nn1/user/$userName/noaccess/"));
	verify_fs_command($fs->ln("hdfs://$nn1/user/$userName/noaccess", "hdfs://$nn1/user/$userName/symlinkdir/noaccess"));
	(undef, undef, undef, $exit_code) = $fs->ls("hdfs://$nn1/user/$userName/symlinkdir/noaccess");
	isnt($exit_code, 0, "Symlinks_27: Shouldn't be able to bypass permissions through a symlink: Blocked by Bug 4423401");
	verify_fs_command($fs->chmod('755', "hdfs://$nn1/user/$userName/noaccess/"));
}

init_test_plan();

# enumerate all tests dynamically by looking at function definitions
my @tests = map { /^(Symlinks_.*)/ ? $1 : () } sort keys (%::);

# run tests
foreach my $function (@tests) {
	no strict 'refs';
	note("$function: Init");
	init_hdfs();
	note("$function: Start");
	$function->();
	note("$function: Cleanup");
	cleanup_hdfs();
	note("$function: Done");
}
