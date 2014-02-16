#!/usr/local/bin/perl
use perlchartdir;
use Time::Local;
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
# This is how the data is expected in the files
# 1st line contains the footprint (basically the spread along the X-Axis)
# Then each subsequent entry is a subsequent run
#
# java.lang.reflect.OneException  java.lang.reflect.OtherException  java.lang.reflect.InvocationTargetException  java.lang.reflect.ThirdInvocationTargetException  org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExists1111Exception  org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException
# Run:Jan_29_2014_22_33_24_996
# 0 0 140 0 0 140
# Run:Jan_31_2014_16_24_47
# 25 0 25 0 0 25
# Run:Jan_31_2014_17_01_24
# 0 0 0 0 1631 0
# Run:Jan_29_2014_22_29_02_718
# 0 0 212 0 0 212
# Run:Jan_31_2014_16_32_26
# 0 35 0 52 20 0
# Run:Jan_31_2014_16_40_39
# 0 162 0 119 1315 0
# Run:Jan_31_2014_16_17_10
# 0 0 19 0 0 19
#####################################################################################

my $file =$inputFileName;
open (FH, "< $file") or die "Can't open $file for read: $!";
my @lines = <FH>;
close FH or die "Cannot close $file: $!";
my $colors = [ 0xff0000,
0x00ff00,
0x0000ff,
0xff00ff,
0xffff00,
0x00ffff,
0x6600CC,
0x660099,
0x999999,
0x999966,
0x999933,
0x9933FF,
0x9933CC,
0x66FF99,
0x99CCFF,
0x66CCFF,
0x00FFFF,
0x000000,
0x660000,
0x003300,
0x666666,
0x99CC00,
0xFF66CC,
0x99FF00,
0x00FFFF,
0x99CC33, ];
my $headingDone=0;
my $xHeadingDone=0;
my $yheadingDone=0;
my $xDataDone=0;
my $yDataDone=0;
my @xlabels;
my @ylabels;
my $count=0;
my @tempArr=();
my $run;
my %xRunAndData;
my %yRunAndData;
foreach my $line (@lines){
	if ($headingDone == 0){
		if($xHeadingDone == 0){
			chomp($line);
			@xlabels = split(/\s/, $line);
			$xHeadingDone=1;
			next;
		}
		if($yheadingDone == 0){
			chomp($line);
			@ylabels = split(/\s/, $line);
			#$yheadingDone=1;
			$headingDone = 1;
			next;
		}
	}
	if ($line =~ m/^Run/){
		chomp($line);
		print "Processing $line\n";
		my @runInfo = split(/:/,$line);
		$run = $runInfo[1];
        print "===========================\n";
        print "Colors size: " . scalar(@$colors) . "\n";
        $hashValue = generateHashCode($run);
        print "Converted date in secs is:" . $hashValue ."\n";
        my $mod = $hashValue % scalar(@$colors);
        print "Adding color: " . @$colors[$mod];
        print "===========================\n";
        $runAndColor{$run}=@$colors[$mod];
		next;
	}else{
		#Data lines now
		if($xDataDone ==0){
			print "Processing X: $line\n";
			chomp($line);
			my @tempArr = split(/\s/,$line);
			$xRunAndData{$run} = \@tempArr;
			$xDataDone=1;
			$yDataDone=0;
			next;
		}
		if($yDataDone ==0){
			print "Processing Y: $line\n";
			chomp($line);
			my @tempArr = split(/\s/,$line);
			$yRunAndData{$run} = \@tempArr;
			$xDataDone=0;
			$yDataDone=1;
			next;
		}
	}
	
}

sub generateHashCode{
    my $dateString = shift;
    my %dateLookup=();
    $dateLookup{"Jan"} = 1;
    $dateLookup{"Feb"} = 2;
    $dateLookup{"Mar"} = 3;
    $dateLookup{"Apr"} = 4;
    $dateLookup{"May"} = 5;
    $dateLookup{"Jun"} = 6;
    $dateLookup{"Jul"} = 7;
    $dateLookup{"Aug"} = 8;
    $dateLookup{"Sep"} = 9;
    $dateLookup{"Oct"} = 10;
    $dateLookup{"Nov"} = 11;
    $dateLookup{"Dec"} = 12;
    my @splits= split(/-----/, $dateString);   #Get rid of the "1-" or "2-" sequences
    $dateString = $splits[1];
    my @dateComponents = split(/_/, $dateString);
    print "Sec = " . $dateComponents[5] . "\n";
    print "Min = " . $dateComponents[4] . "\n";
    print "Hour = " . $dateComponents[3] . "\n";
    print "day = " . $dateComponents[1] . "\n";
    print "Month = " . $dateComponents[0] . "\n";
    print "Year = " . $dateComponents[2] . "\n";
    my $hash = 1;
    $hash = $hash * 17 + $dateComponents[0];
    $hash = $hash * 17 + $dateComponents[1];
    $hash = $hash * 17 + $dateComponents[2];
    $hash = $hash * 17 + $dateComponents[3];
    $hash = $hash * 17 + $dateComponents[4];
    $hash = $hash * 17 + $dateComponents[5];
    
    return $hash;
    
}


sub convertDateIntoNumber{
    my $dateString = shift;
    my %dateLookup=();
	$dateLookup{"Jan"} = 1;
	$dateLookup{"Feb"} = 2;
	$dateLookup{"Mar"} = 3;
	$dateLookup{"Apr"} = 4;
	$dateLookup{"May"} = 5;
	$dateLookup{"Jun"} = 6;
	$dateLookup{"Jul"} = 7;
	$dateLookup{"Aug"} = 8;
	$dateLookup{"Sep"} = 9;
	$dateLookup{"Oct"} = 10;
	$dateLookup{"Nov"} = 11;
	$dateLookup{"Dec"} = 12;
    my @splits= split(/-/, $dateString);   #Get rid of the "1-" or "2-" sequences
    $dateString = $splits[1];
    my @dateComponents = split(/_/, $dateString);
    print "Sec = " . $dateComponents[5] . "\n";
    print "Min = " . $dateComponents[4] . "\n";
    print "Hour = " . $dateComponents[3] . "\n";
    print "day = " . $dateComponents[1] . "\n";
    print "Month = " . $dateComponents[0] . "\n";
    print "Year = " . $dateComponents[2] . "\n";
    my $seconds = timelocal($dateComponents[5], $dateComponents[4], $dateComponents[3], $dateComponents[1], $dateLookup{$dateComponents[0]}, $dateComponents[2]);
    return $seconds;
}
print "Checking:" . $xRunAndData{$run} . "\n";
print "_____________________________________________\n";
print "Checking:" . $yRunAndData{$run} . "\n";

my $symbols = [
1,
2,
3,
4,
5,
6,
7,
15,
16,
17,
perlchartdir::StarShape(5),
perlchartdir::StarShape(6),
perlchartdir::StarShape(7),
perlchartdir::StarShape(8),
perlchartdir::StarShape(9),
perlchartdir::StarShape(10),
perlchartdir::PolygonShape(5),
perlchartdir::PolygonShape(6),
perlchartdir::Polygon2Shape(5),
perlchartdir::Polygon2Shape(6),
perlchartdir::CrossShape(6), ];

# Create an XYChart object of size 900 x 500 pixels, with a light blue (EEEEFF)
# background, black border, 1 pxiel 3D border effect and rounded corners
#my $c = new XYChart(1800, 800, 0xeeeeff, 0x000000, 1);
my $c = new XYChart(2500, 1500, 0xeeeeff, 0x000000, 1);
#$c->setRoundedFrame();

# Set the plotarea at (55, 58) and of size 520 x 195 pixels, with white background.
# Turn on both horizontal and vertical grid lines with light grey color (0xcccccc)
$c->setPlotArea(250, 88, 2200, 1200, 0xffffff, -1, -1, 0xcccccc, 0xcccccc);

# Add a legend box at (50, 30) (top of the chart) with horizontal layout. Use 9 pts
# Arial Bold font. Set the background and border color to Transparent.
$c->addLegend(200, 40, 0, "arialbd.ttf", 9)->setBackground($perlchartdir::Transparent) ;

# Add a title box to the chart using 15 pts Times Bold Italic font, on a light blue
# (CCCCFF) background with glass effect. white (0xffffff) on a dark red (0x800000)
# background, with a 1 pixel 3D border.
$c->addTitle($chart_title, "timesbi.ttf", 15);

# Add a title to the y axis
$c->yAxis()->setTitle($y_axis_title, "arialbi.ttf", 12);
$c->xAxis()->setTitle($x_axis_title, "arialbi.ttf", 12);

# Set axis line width to 2 pixels
$c->xAxis()->setWidth(3);
$c->yAxis()->setWidth(3);

# Set the labels on the x axis.
#print "Setting labels" . join("|",@labels);
$c->xAxis()->setLabels(\@xlabels);
$c->xAxis()->setLabelStyle("",8,TextColor, "90");
$c->yAxis()->setLabels(\@ylabels);
$c->yAxis()->setLabelStyle("",8,TextColor, "0");

# Display 1 out of 2 labels on the x-axis.
$c->xAxis()->setLabelStep(1);
$c->yAxis()->setLabelStep(1);

# Add the three data sets to the line layer. For demo purpose, we use a dash line
# color for the last line
print "==============================================\n";
$count=0;
foreach my $runn (keys (%xRunAndData)){
	#$c->addScatterLayer($xRunAndData{$runn}, $yRunAndData{$runn}, "Run-".$runn, $$symbols[$count], 15, $$colors[$count]);
	$c->addScatterLayer($xRunAndData{$runn}, $yRunAndData{$runn}, "Run-".$runn, $$symbols[$count], 15, $runAndColor{$runn});
	$count++;
	print "Loop-".$runn."\n";
}

# Output the chart
$c->makeChart($graphFileName);