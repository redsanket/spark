#!/usr/bin/env perl
# Simple util to dump twiki docs for simon metrics

use constant {
  FORMAT => "| %32s | %-64s |\n",
};

my $reportName;
my $inComment = 0;

# Not a real XML parser, but enough to grab what we want
while (<>) {
  /<!--/ && do { $inComment = 1 };
  /-->/ && do { $inComment = 0 };
  next if $inComment;

  /<report\s+name="([^"]+)"/ && do { $reportName = $1; next; };
  /<reportItem\s+name="([^"]+)"\s+blurb="([^"]+)"/ && do {
    print "---+++ SIMON Metrics for $reportName '$2' ($1)\n";
    printf FORMAT, "*SIMON Metric*", "*Function of Original Metric*";
    next;
  };
  /<value\s+name="([^"]+)"[^>]*>\s*([^<]+)/ && do {
    printf FORMAT, "!$1", "!$2";
    next;
  }
}
