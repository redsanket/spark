#!/usr/local/bin/perl
use perlchartdir;
#use strict;

my %options=();
my ($chart_title, $x_axis_title, $y_axis_title, $graphFileName, $inputFileName);

#
# Command line options processing
#
use Getopt::Long;
&Getopt::Long::Configure();
my $result = GetOptions(\%options,
"t|chart_title=s"         => \$chart_title,
"x|x=s"          => \$x_axis_title,
"y|y=s"          => \$y_axis_title,
"i|input_file_name=s"          => \$inputFileName,
"o|graph_file_name=s"          => \$graphFileName,
"help|h|?"
);

#####################################################################################
#This is how this script expects input data.
#Each row is the count of jobs in a particular state, across differnet runs
#Ist row=SUCCEEDED, 2nd row=FAILED, 3rd row=KILLED 4th row = UNKNOWN
#Feb_07_2014_17_03_44 Feb_07_2014_17_17_06 Feb_07_2014_17_23_26 Feb_07_2014_17_29_33 Feb_07_2014_17_35_02 Feb_07_2014_17_40_28 Feb_07_2014_17_46_08
#70 70 70 70 70 70 70
#0 0 0 0 0 0 0
#0 0 0 0 0 0 0
#0 0 0 0 0 0 0
#####################################################################################

my $file =$inputFileName;
open (FH, "< $file") or die "Can't open $file for read: $!";
my @lines = <FH>;
close FH or die "Cannot close $file: $!";
my $headingDone=0;
my @labels;
my @succeeded;
my @failed;
my @killed;
my @unknown;
my $count=0;
my $run;
my %runAndData;
foreach my $line (@lines){
	if ($headingDone == 0){
		$headingDone = 1;
		$line =~ s/^\s+|\s+$//g;
		@labels = split(/\s+/, $line);
		$count++;
		next;
	}
	if ($count==1){
		print "Processing succeeed status $line";
		@succeeded = split(/\s+/,$line);
		$count++;
		next;
	}
	if ($count==2){
		print "Processing failed status $line";
		@failed = split(/\s+/,$line);
		$count++;
		next;
	}
	if ($count==3){
		print "Processing killed status $line";
		@killed = split(/\s+/,$line);
		$count++;
		next;
	}
	if ($count==4){
		print "Processing unknown status $line";
		@unknown = split(/\s+/,$line);
		$count++;
		next;
	}
	
}

# Create an XYChart object of size 900 x 500 pixels, with a light blue (EEEEFF)
# background, black border, 1 pxiel 3D border effect and rounded corners
my $c = new XYChart(1800, 1000, 0xeeeeff, 0x000000, 1);

$c->setPlotArea(55, 58, 1720, 500, 0xffffff, -1, -1, 0xcccccc, 0xcccccc);

# Add a legend box at (50, 30) (top of the chart) with horizontal layout. Use 9 pts
# Arial Bold font. Set the background and border color to Transparent.
$c->addLegend(50, 30, 0, "arialbd.ttf", 9)->setBackground($perlchartdir::Transparent) ;

# Add a title box to the chart using 15 pts Times Bold Italic font, on a light blue
# (CCCCFF) background with glass effect. white (0xffffff) on a dark red (0x800000)
# background, with a 1 pixel 3D border.
$c->addTitle($chart_title, "timesbi.ttf", 20)->setBackground(
0xccccff, 0x000000, perlchartdir::glassEffect());

# Add a title to the y axis
$c->yAxis()->setTitle($y_axis_title);

# Set axis label style to 8pts Arial Bold
$c->xAxis()->setLabelStyle("timesbd.ttf", 10);
$c->yAxis()->setLabelStyle("timesbd.ttf", 8);

# Set the labels on the x axis.
#print "Setting labels" . join("|",@labels);
$c->xAxis()->setLabels(\@labels);
$c->xAxis()->setLabelStyle("",8,TextColor, "45");


# Add a line layer to the chart
my $layer = $c->addBarLayer2($perlchartdir::Side, 3);

$layer->addDataSet(\@succeeded, 0x80ff80, "Succeeed");
$layer->addDataSet(\@failed, 0xff8080, "Failed");
$layer->addDataSet(\@killed, 0x8080ff, "Killed");
$layer->addDataSet(\@unknown, 0x6600CC, "Unknown");

# Output the chart
$c->makeChart($graphFileName);