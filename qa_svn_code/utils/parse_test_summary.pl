#!/usr/local/bin/perl

# Example:
# /homes/philips/svn/HadoopQEAutomation/branch-20/utils/parse_test_summary.pl \
# -d /home/y/var/builds/workspace/NightlyHadoopQEAutomation-20/artifacts-regression-hdfs \
# -d /home/y/var/builds/workspace/NightlyHadoopQEAutomation-20/artifacts-regression-mapreduce/ \
# -d /home/y/var/builds/workspace/HadoopQERegression-DFIP/artifacts/

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../lib";
use Getopt::Long;
use Test::More;
use Text::Trim;
use Math::Round;

my %options = ();
my @artifacts_dirs;
my $verbose = 0;
my $result  = 
    GetOptions ( \%options,
                 "dir|d=s{,}" => \@artifacts_dirs,
                 "verbose"    => \$verbose,
                 "help|h|?");


die("Error: mising required artifacts dir") unless (@artifacts_dirs);
usage(1) if (!$result) or (@ARGV);
usage(0) if $options{help};

sub usage
{
    my($exit_code) = @_;
    print STDERR << "EOF";
Usage: $Script
   -dir|d <artifacts_dir> ... : one or more artifacts directory
   -verbose                   : verbose mode
   [-help|h|?             ]   : Display usage

EOF
    exit $exit_code;
}

my $product = 'Hadoop';
my $version = '0.20.25';
my @all_versions = ();
my @bugs_desc_array = ();
my @failed_components = ();
my @output_total = ();
my @output_features = ();
my $component_sum = {};

my @output_toc = ("<strong>Table of Contents </strong><br />",
                  "%TWISTY{",
                  "mode=\"div\"",
                  "showlink=\"More \"",
                  "hidelink=\"Hide \"",
                  "}%",
                  "%TOC% ",
                  "%ENDTWISTY%");

# Parse the test summary for each of the artifact directories that are passed in
my @result_types = ('pass', 'fail', 'skip');    
my $agg_sum = {};
foreach my $type (@result_types) { $agg_sum->{$type} = 0; }

#push(@output_total, "---++ Feature Totals\n");
push(@output_features, "\n---++ Feature Summary\n");

foreach my $artifacts_dir (@artifacts_dirs) {
    next if ($artifacts_dir =~ 'artifacts-setup-hudson-aggregation');
    my $component_summary = parse_test_summary($artifacts_dir, $verbose);
    foreach my $result_type (@result_types) {
        $agg_sum->{$result_type} +=
            $component_summary->{result_sum}->{$result_type};
    }
}

sub uniq {
    return keys %{{ map { $_ => 1 } @_ }};
}
@all_versions = uniq(@all_versions);
my $version_str = join(", ", @all_versions);

# Header section
print("---+ $product Test Results for '$version_str'\n");
print("\n@output_toc\n\n");

# Overall total section
my $header = 
    "| *Version* | *Component* | *Status* | *Total* | *Pass* | *Fail* ".
    "| *Skip* | *%Pass* | *%Fail* | *%Skip*| *Comments* |\n";
unshift(@output_total, $header);
unshift(@output_total, "'%STARTSECTION{\"Features_Total\"}%\n");
push(@output_total, "%ENDSECTION{\"Features_Total\"}%\n");

my $component = 'Overall Total';
unshift(@output_total, "%ENDSECTION{\"Overall_Total\"}%\n");

use File::Basename;
my $artifacts_dir = dirname($artifacts_dirs[0])."/artifacts";
my $dir="$artifacts_dir/TestSummary";
my $test_info_log = "$dir/testInfo.log";
trim(my $build_url = `grep BUILD_URL $test_info_log|awk '{print \$3}'`);
my $component_summary = { 'name'           => 'Overall Total',
                          'build_url'      => $build_url,
                          'result_sum'     => $agg_sum, 
                          'hadoop_version' => $version_str };
unshift(@output_total, 
        construct_overall_summary($component_summary,
                                  \@failed_components,
                                  \@bugs_desc_array));
unshift(@output_total, $header);
unshift(@output_total, "%TABLE{name=TestSummary}%\n");

my $time = localtime;
unshift(@output_total, "%REVINFO{Last script update date: $time}%\n");
unshift(@output_total, '%REVINFO{Last twiki update date: $date}% <br>'."\n");

unshift(@output_total, "%STARTSECTION{\"Overall_Total\"}%\n");
unshift(@output_total, "---++ $component\n");
print(@output_total);

# Feature summary section
print(@output_features);

sub construct_overall_summary {
    my ($component_summary, $failed_components, $bugs_desc_array) = @_;
    my $component  = $component_summary->{name};
    my $result_sum = $component_summary->{result_sum};
    my @output = ();
    my $agg_total=0;
    foreach my $key (sort(keys %$result_sum)) {
        $agg_total += $result_sum->{$key};
    }    
    my $percent_passed  = 0;
    my $percent_failed  = 0;
    my $percent_skipped = 0;
    if ($agg_total > 0) {
        $percent_passed  = nearest(1,($result_sum->{pass}/$agg_total)*10000)/100;
        $percent_failed  = nearest(1,($result_sum->{fail}/$agg_total)*10000)/100;
        $percent_skipped = nearest(1,($result_sum->{skip}/$agg_total)*10000)/100;
    }
    my $status = $result_sum->{fail} ? 'yellow' : 'green';

    my $component_str = $component;
    my $status_str    = "%ICON{led-$status}%";
    my $agg_total_str = $agg_total;
    if (exists $component_summary->{build_url}) {
        my $build_url = "$component_summary->{build_url}";
        $component_str = "[[$build_url][$component]]";
        $status_str    = "[[$build_url/consoleText][$status_str]]";
        $agg_total_str = "[[$build_url/testReport/regression/tap/][$agg_total]]";
    }
    my $version_str = join(", <BR>", split(",", $component_summary->{hadoop_version}));
    my $str =
        "| $version_str | $component_str | $status_str | $agg_total_str ".
        "| $result_sum->{pass} | $result_sum->{fail} | $result_sum->{skip} ".
        "| $percent_passed% | $percent_failed% | $percent_skipped% |";
    
    # Construct the comment field 
    my @comments;

    push(@comments, "%ICON{warning}% Tests still in progress at the time of report...")
        if $component_summary->{is_still_running};

    if ($failed_components && @$failed_components) {
        my $aux_str = '%X% '.scalar(@$failed_components)." failed comoponent(s):";
        foreach my $comp (@$failed_components) {
            $aux_str .= "<BR> $comp (".$component_sum->{$comp}->{fail}.") - need investigation";
        }
        push(@comments, $aux_str);
    }
    if ($bugs_desc_array && @$bugs_desc_array) {
        push(@comments,
             '%X%'." $agg_sum->{skip} test(s) skipped due to ".
             scalar(@$bugs_desc_array)." bug(s): <BR>@$bugs_desc_array");        
    }
    $str .= ' '.join('<BR>', @comments);
    $str .= ' |'; 
    push(@output, "$str\n");
    return @output;
}

sub construct_feature_summary {
    my ($component_summary, $known_bugs) = @_;
    my $component   = $component_summary->{name};
    my $feature_sum = $component_summary->{feature_sum};

    push(@output_features, "%TABLE{name=TestSummary_$component"."_PerFeature}%\n");
    push(@output_features, "| *Feature* | *Status* | *Total* | *Pass* | *Fail* | *Skip* | *Comments* |\n");
    foreach my $key (sort(keys %$feature_sum)) {
        my $str="";
        my $total=0;
        my $status='yellow';
        foreach my $type (@result_types) {
            if (exists $feature_sum->{$key}->{$type}) {
                $total += $feature_sum->{$key}->{$type};
                $str .= " $feature_sum->{$key}->{$type} |";
            }
            else {
                $str .= " 0 |";
            }

            if ($type eq 'fail') {
                $status = 'green' unless (exists $feature_sum->{$key}->{$type});
            }

            if ($type eq 'skip') {
                if (exists $feature_sum->{$key}->{$type}) {
                    $str .= $known_bugs->{$key};
                }
            }
        }
        push(@output_features, "| $key | %ICON{led-$status}% | $total | $str |\n");
    }     
    push(@output_features, "\n");
}

sub parse_test_summary {
    my ($artifacts_dir, $verbose) = @_;
    my $component_summary = {};

    my @tokens = split("/", $artifacts_dir);
    my $component = uc($tokens[-1]);
    $component=~s/artifacts-//i;

    $component_summary->{name} = $component;
    # push(@output_total, "---+++ $component\n");
    push(@output_features, "---+++ $component\n");

    my $dir="$artifacts_dir/TestSummary";
    # hash reference of sum for overall test results 'pass', 'fail', or 'skip'
    my $result_sum = {};
    # hash reference of sum sum for each feature
    my $feature_sum = {};
    my $known_bugs = {};
    my $bug_file="$Bin/../conf/test/known_bugs.txt";

    # Maintenance Note / TODO:
    # We need to check if the test has even started or not
    # If not, we need to hanle this differently
    my $is_started = (-d $dir) ? 0 : 1;


    # The testSummary.log is generated at the completion of the test execution.
    # If it's not there, the tests are still running
    my $test_summary_log = "$dir/testSummary.log";
    my $is_still_running = (-f $test_summary_log) ? 0 : 1;

    # Get the hadoop version
    my $test_info_log = "$dir/testInfo.log";
    trim(my $hadoop_version = `/bin/grep -i VERSION $test_info_log|awk '{print \$5}'`);
    $component_summary->{hadoop_version} = $hadoop_version;
    push(@all_versions, $hadoop_version);

    # Get the build URL
    trim(my $build_url = `grep BUILD_URL $test_info_log|awk '{print \$3}'`);
    $component_summary->{build_url} = $build_url;

    # note("---> Parse test summary from artifacts directory '$dir':");
    $component_sum->{$component} = {};
    foreach my $result_type (@result_types) {
        parse_artifact_file($dir, $bug_file, $result_type, $result_sum, $feature_sum,
                            $known_bugs, $verbose);
    }
    $component_sum->{$component} = $result_sum;
    push(@failed_components, $component) if ($result_sum->{fail});

    $component_summary->{result_sum} = $result_sum;
    $component_summary->{feature_sum} = $feature_sum;
    $component_summary->{is_still_running} = $is_still_running;

    push(@output_total, construct_overall_summary($component_summary));
    construct_feature_summary($component_summary, $known_bugs);
    return $component_summary;
}

sub parse_artifact_file {
    my ($dir, $bug_file, $type, $result_sum, $feature_sum, $known_bugs, $verbose) = @_;

    $result_sum->{$type} = 0;
    my $file = "$dir/$type"."Summary.log";
    if (-f $file) {
        # note("---> File '$file' exists:");
        trim($result_sum->{$type} = `cat $file|wc -l`);
        # note("sum for $type = $result_sum->{$type}"."\n");
        my $test_suite_str = `cut -d ':' -f1  $file|sort -u`;
        note("test_suite_str='$test_suite_str'") if $verbose;
        my @test_suites = split('\n', $test_suite_str);

        my $all_bugs = {};
        foreach my $suite (@test_suites) {
            my $count = `/bin/grep -c $suite $file`;
            trim($count);
            $feature_sum->{$suite}->{$type} = $count;
            # Fetch the known bug id associated with skip
            if ($type eq 'skip') {
                # Get the test case names
                my @tc_names = split("\n", `/bin/grep $suite $file|cut -d ':' -f2|cut -d '|' -f1`);
                my $ts_bugs = {};
                foreach my $tc (@tc_names) {
                    note("grep '$tc' $bug_file\n") if $verbose;

                    my $result = `grep '$tc' $bug_file`;

                    my $bug;
                    if ($result) {
                        my @tokens = split(" ", $result);
                        $bug = $tokens[2] || 'undef';
                        trim($bug);
                    }
                    else {
                        note("no bug id associated with the skipped testcase '$tc'") if $verbose;
                        $bug = 'intermittent';
                    }

                    note("Known Bugs associated with suite '$suite', ".
                         "testcase '$tc' is '$bug") if $verbose;

                    # Add the test case to the test suite bugs and all bugs hash
                    $ts_bugs = add_tc_to_bugs_hash($ts_bugs, $bug, $tc);
                    $all_bugs = add_tc_to_bugs_hash($all_bugs, $bug, $tc);
                }

                note("test suite bugs = ",explain($ts_bugs)) if $verbose;
                note("all_bugs = ",explain($all_bugs)) if $verbose;;

                my @ts_bugs_desc_array = @{format_bugs_hash_to_array($ts_bugs)};
                note("all ts bugs array = ", explain(\@ts_bugs_desc_array))
                    if $verbose;
                $known_bugs->{$suite} = "@ts_bugs_desc_array";
            }
        }

        if ($type eq 'skip') {
            note("known bugs = ", explain($known_bugs)) if $verbose;
            @bugs_desc_array = @{format_bugs_hash_to_array($all_bugs)};
            note("all bugs array = ", explain(\@bugs_desc_array)) if $verbose;
        }
    }
    else {
        # note("---> File '$file' does not exist:");
    }
}

sub add_tc_to_bugs_hash {
    my ($bugs, $bug, $tc) = @_;
    if (exists $bugs->{$bug}) {
        push(@{$bugs->{$bug}}, "$tc"); 
    }
    else {
        @{$bugs->{$bug}} = ("$tc");
    }
    return $bugs
}

sub format_bugs_hash_to_array {
    my ($bugs_hash) = @_;
    my $bugs_array = [];
    note("bugs hash = ", explain($bugs_hash)) if $verbose;
    foreach my $key (sort(keys %$bugs_hash)) {
        # If bugs hash element is an array of size greater than 1, insert a RS
        my $RS = (scalar(@{$bugs_hash->{$key}}) > 1) ? '<BR>' : '';
        my $str = "Bug:$key : $RS".join(",<BR>", @{$bugs_hash->{$key}})." <BR>";
        note("bugs hash key = '$key', str = '$str'") if $verbose;
        push(@$bugs_array, "$str");
    }
    note("bugs array = ", explain($bugs_array)) if $verbose;
    return $bugs_array;
}
