package Hadoop::HtmlTables;

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";

use Test::More;
use Hadoop::Test;
use Hadoop::Config;
use base qw(HTML::Parser);
use LWP::Simple;


=head2 new

Instantiates a new HtmlTables object (which in turn instantiates the HTML::Parser object)

=over 2

=item I<Parameters>:

   * Hadoop::Test object

=item I<Returns>:

   * Blessed instance of the HtmlTables object

=back
=cut
sub new {
    my ($class, $test) = @_;

    my $self = HTML::Parser->new(
        api_version => 3,
        unbroken_text => 1,
        start_h => [\&_start, 'self, tagname, text, attr'],
        end_h => [\&_end,   'self, tagname, attr'],
    );

    $self->{test} = $test;
    $self->{config} = $test->{config};
    $self->{tables} = {}; # Generated table hash
    $self->{current_header} = {}; # Keeps track of data under current header (h1, h2, ...)
    #current_row; # Keeps track of the row in the table
    #current_href; # Keeps track of the current href link
    #in_tr; # keeps track of nested <tr> to allow skipping nested table/tr information

    bless $self, $class;
};


# If we are in an <a> block, convert the text to a hashref (href => text)
sub _check_href {
    my ($self, $text) = @_;
    return { $text => $self->{current_href} } if defined($self->{current_href});
    return $text;
}

# If we have a table beneath a header section, parse the text into the tables hash
sub _parse_table_text {
    my ($self, $text) = @_;
    return unless (exists($self->{current_header}{header}) && defined($self->{current_row}));
    chomp $text;
    if ( ref($self->{tables}{$self->{current_header}{text}}) ) { # The current hashref & arrayref exist, so just add the text
        push(@{$self->{tables}{$self->{current_header}{text}}{rows}[$self->{current_row}]}, $self->_check_href($text));
    } else { # We have not created the array ref for the $self->{current_row} yet
        $self->{tables}{$self->{current_header}{text}}{rows} = [];
        $self->{tables}{$self->{current_header}{text}}{rows}[$self->{current_row}] = [$self->_check_href($text)];
    }
}

# Saves the current header's text (e.g. <h2>THIS TEXT HERE</h2>)
sub _parse_header {
    my ($self, $text) = @_;
    chomp $text;
    $self->{current_header}{'text'} = $text;
}

# If we find a <table>, make sure we report all table tags (if we're not already in a <tr>)
# If we find a header (<h1>, <h2>, ...), save it and change the 'text' parser to handle it
# If we find a <tr>, turn off the text parser, report only table element tags, and increment the current_row
# If we find a <a>, turn on the text parser, and save the href
# If we find a table element (<td>, <th>), turn on the text parser
sub _start {
    my ($self, $html_tag, $text) = @_;
    chomp $text;

    # We're ignoring tables and rows while we're in a <tr> block
    if ($html_tag =~ /^table$/i && !$self->{in_tr}) {
        $self->report_tags( qw(table th td tr a) );
    } elsif ($html_tag =~ /^h[123456]$/i) {
        $self->{current_header} = {header => $html_tag};
        $self->handler('text' => \&_parse_header, 'self, text');
    } elsif ($html_tag eq 'tr' && !$self->{in_tr}++) {
        $self->handler('text' => '');
        $self->report_tags( qw(th td tr a) );
        $self->{current_row}++;
    } elsif ($html_tag eq 'a') {
        $self->handler('text' => \&_parse_table_text, 'self, text');
        $text =~ /^\<a href="?(.*?)"?>$/i; # non-greedy match
        $self->{current_href} = $1;
    } elsif ($html_tag =~ /^t[hd]$/) {
        $self->handler('text' => \&_parse_table_text, 'self, text');
    }
}

# If we find a closing header tag (</h1>, </h2>, ...), turn off the text parser
# If we find a </table>, turn off the text parser, and turn on reporting of tables & headers
# If we find a </tr>, decrement the in_tr counter, turn off the text parser, and turn on reporting tables and <tr>
# If we find a </a>, just undefine the current_href so we're not in a link
# If we find a closing table element (</td>, </th>), turn off the text parser
sub _end {
    my ($self, $html_tag) = @_;
    if ($html_tag eq $self->{current_header}{header}) {
        $self->handler('text' => '');
    } elsif ($html_tag eq 'table') {
        $self->{current_row} = 0;
        $self->handler('text' => '');
        $self->report_tags( qw(table h1 h2 h3 h4 h5 h6) );
    } elsif ($html_tag eq 'tr') {
        $self->{in_tr}--;
        $self->handler('text' => '');
        $self->report_tags( qw(table tr) ) unless $self->{in_tr};
    } elsif ($html_tag eq 'a') {
        $self->{current_href} = undef;
    } elsif ($html_tag =~ /^t[hd]$/) {
        $self->handler('text' => '');
    }
}

=head2 read_html($self, $uri)

Read the text from the specified URI and return it as a string

=over 2

=item I<Parameters>:

   * uri of the web page containing the tables to parse

=item I<Returns>:

   * String of the html from the specified uri

=back

=cut
sub read_html {
    my ($self, $uri) = @_;
    note("Reading uri: '$uri'");
    my $html = LWP::Simple::get($uri);
    return $html;
}


=head2 get_html_tables($self, $uri)

Uses the id for the h2 (or hx) header immediately before the table if present.
Currently ignored tables that have not had a header given before.  (it would
be better to at least check whether the table has its own id or something).

=over 2

=item I<Parameters>:

   * uri of the web page containing the tables to parse

=item I<Returns>:

   * hash-ref of all the header-id => table-hash-ref

=back

=cut
sub get_html_tables {
    my ($self, $uri) = @_;
    my $html = $self->read_html($uri);

    # Cleanup any previous runs
    $self->{tables} = {};
    $self->{current_header} = {};
    delete($self->{current_row}) if exists($self->{current_row});
    delete($self->{current_href}) if exists($self->{current_href});
    delete($self->{in_tr}) if exists($self->{in_tr});

    return unless $html;
    $self->report_tags(qw(table h1 h2 h3 h4 h5 h6));
    $self->handler('text' => '');
    $self->parse($html);
    $self->eof;

    return $self->{tables};
}


=head2 get_$component_html_tables($self) <get_jobtracker_html_tables, get_namenode_html_tables, get_datanode_html_tables, get_tasktracker_html_tables, get_gateway_html_tables>

Looks up the specified component hosts and returns a hash of all the html tables found on its corresponding web page
Note: Some of the components don't have the necessary information in our config object yet (missing port info)

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Hash-ref containing all the host -> tables from the corresponding html pages

=back
=cut
foreach my $component ('namenode', 'secondary_namenode', 'datanode', 'tasktracker', 'jobtracker', 'gateway') {
    no strict 'refs';
    my $comp_method = "get_$component";
    my $uri_method = "get_${component}_uri";

    # sub get_jobtracker_html_tables, sub get_namenode_html_tables, sub get_datanode_html_tables, sub get_tasktracker_html_tables, sub get_gateway_html_tables
    *{"get_${component}_html_tables"} = sub {
        my ($self) = @_;
        my $hosts = $self->{config}->$comp_method();
        my %tables;
        foreach my $host (ref($hosts) ? @$hosts : ($hosts)) {
            my $host = $self->{config}->{NODES}->{JOBTRACKER}->{HOST};
            my $port = $self->{config}->{NODES}->{JOBTRACKER}->{PORT};
            my $uri = "http://$host:$port";
            $tables{$host} = $self->get_html_tables($self->{config}->$uri_method($host));
        }
        return \%tables;
    };
}

1;
