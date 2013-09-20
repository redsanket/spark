#!/home/y/bin/perl -w

use strict;
use Data::Dumper;
use MIME::Lite;
use Getopt::Long;

my %opts;

# Default values
$opts{mailto} = "hadoop-hit\@yahoo-inc.com";
$opts{title} = "Hadoop/HIT Stack Deployment Complete";


# Get CLI options
GetOptions(
    "title=s" => \$opts{title},
    "mailto=s" => \$opts{mailto},
);


$opts{build_url} = $ENV{BUILD_URL};
$opts{hudson_url} = $ENV{HUDSON_URL};
$opts{job_name} = $ENV{JOB_NAME};
$opts{build_number} = $ENV{BUILD_NUMBER};

my $htmltext = get_html_header();

# Summary table
$htmltext .= "
<div id=summary>
  <table class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"55%\">
    <tr valign=\"top\">
        <th>Parameter</th>
        <th>Value</th>
    </tr>
    <tr>
        <td>Job Name</td>
        <td>$opts{job_name}</td>
    </tr>
    <tr>
        <td>Build Number</td>
        <td>$opts{build_number}</td>
    </tr>
    <tr>
        <td>Test Report</td>
        <td><a href=\"$opts{build_url}/testReport\">Click Here</a></td>
    </tr>
    <tr>
        <td>Log Details</td>
        <td><a href=\"$opts{build_url}/consoleFull\">Click Here</a></td>
    </tr>
  </table>
</div>
";


$htmltext .= "</table>\n<p/>\n";

# read from mnifest file and get component details
my $manifest = $ENV{WORKSPACE}. "/manifest.txt";
if (-e $manifest) 
{
    # Read from manifest file
    my ($fh, @result);
    open $fh, $manifest or die  "cannot open file '$manifest': $!";
    foreach (<$fh>)
    {
        next if (m/^#/);
        if (m/^pkgname=(.*)\s+pkgver=(.*)$/)
        {
            push @result, { "pkgname" => $1, pkgver => $2 }; 
        } else {
            my @split_list = split(" ",$_);
            foreach (@split_list)
            {
                if (m/(.*)-(\d+\.\d+(.*))/) {
                    # deployment from a version string
                    push @result, { "pkgname" => $1, pkgver => $2 };
                } elsif (m/(.*):(.*)/) {
                    # deployment from a tag
                    push @result, { "pkgname" => $1, pkgver => $2 };
                }
            }
        }
    }    

    if (@result)
    {
        $htmltext .= "<hr size=\"1\"/>\n<h1>Component Details</h1>\n<div id=\"details\">\n";
        $htmltext .= "<table  class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"95%\">\n";
        $htmltext .= "    <tr valign=\"top\">\n";
        $htmltext .= "        <th>Package</th>\n";
        $htmltext .= "        <th>Version/Tag</th>\n";
        $htmltext .= "    </tr>\n"; 
        foreach my $ret (@result)
        {
            $htmltext .= "    <tr valign=\"top\">\n";
            $htmltext .= "        <td>" . $ret->{pkgname} . "</td>\n";
            $htmltext .= "        <td>" . $ret->{pkgver} . "</td>\n";
            $htmltext .= "    </tr>\n"; 
        }
        $htmltext .= "</table>\n";
        $htmltext .= "</div>\n";
    }
}

$htmltext .= "</body>\n</html>\n";

$opts{mailto} = join(",", split_list($opts{mailto}));

my $msg = MIME::Lite->new
            (
               Subject => $opts{title},
               From    => "HIT Deployment Notification <do.not.reply\@yahoo-inc.com>",
               To      => $opts{mailto},
               Type    => "text/html",
               Data    => $htmltext,
            );

print "Sending report by email to '$opts{mailto}' now......\n\n";
$msg->send();

sub get_html_header
{
    my $html_header = "
<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">
<html>
<head>
<META http-equiv=\"Content-Type\" content=\"text/html; charset=US-ASCII\">
<title>$opts{title}</title>
<style type=\"text/css\">

body {
    font:normal 68% verdana,arial,helvetica;
    color:#000000;
}

table tr td, table tr th {
    font-size: 68%;
}

table.details tr th{
    font-weight: bold;
    text-align:left;
    background:#a6caf0;
    white-space: nowrap;
}

table.details tr td{
    background:#eeeee0;
    white-space: nowrap;
}

h1 {
    margin: 0px 0px 5px; font: bold 165% verdana,arial,helvetica
}

h2 {
    margin-top: 1em; margin-bottom: 0.5em; font: bold 125% verdana,arial,helvetica
}

h3 {
    margin-bottom: 0.5em; font: bold 115% verdana,arial,helvetica
}

.Failure {
    font-weight:bold; color:red;
}

.Pass {
    font-weight:bold; color:green;
}

img
{
    border-width: 0px;
}
				
.expand_link
{
    position=absolute;
    right: 0px;
    width: 27px;
    top: 1px;
    height: 27px;
}


.page_details
{
    display: none;
}

.page_details_expanded
{

    display: block;
    display/* hide this definition from  IE5/6 */: table-row;
}


</style>
<script language=\"JavaScript\">
			   function expand(details_id)
			   {
			      
			      document.getElementById(details_id).className = \"page_details_expanded\";
			   }
			   
			   function collapse(details_id)
			   {
			      
			      document.getElementById(details_id).className = \"page_details\";
			   }
			   
			   function change(details_id)
			   {
			      if(document.getElementById(details_id+\"_image\").src.match(\"expand\"))
			      {
			         document.getElementById(details_id+\"_image\").src = \"collapse.jpg\";
			         expand(details_id);
			      }
			      else
			      {
			         document.getElementById(details_id+\"_image\").src = \"expand.jpg\";
			         collapse(details_id);
			      } 
			   }
			</script>
</head>
<body>
    <h1>$opts{title}</h1>
    <p/>
";

    return $html_header;
}


#-------------------------------------------
# helper function to split a string of list
#-------------------------------------------
sub split_list
{
    my $list = shift;
    my $i;
    $list =~ s/\n//osg;
    $list =~ s/\s+$//os;
    my @results = split /\s*(?<!\\),\s*/so, $list; #zero width look behind
    $results[0] =~ s/^\s*//o;
    map {s:\\,:,:og} @results;
    return @results;
}
