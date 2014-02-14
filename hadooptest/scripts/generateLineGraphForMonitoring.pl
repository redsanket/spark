#!/usr/local/bin/perl
use perlchartdir;
use Time::Local;

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
my $headingDone=0;
my @labels;
my $count=0;
my $run;
my %runAndData;
my %runAndColor;
my %runAndSymbol;
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


foreach my $line (@lines){
	if ($headingDone == 0){
		$headingDone = 1;
		$line =~ s/^\s+|\s+$//g;
		@labels = split(/\s+/, $line);
		$count = $count+1;
		next;
	}
	if ($line =~ m/^Run/){
		$line =~ s/^\s+|\s+$//g;
		print "Processing $line";
		@runInfo = split(/:/,$line);
		$run = $runInfo[1];
        if ($run =~ /([a-zA-Z]+)_([0-9]+)_.*/){
            print $1 ." ". $2 . "\n";
            print "===========================\n";
            print "Colors size: " . scalar(@$colors) . "\n";
            $hashCodeForColor = generateHashCodeToDetermineLineColor($run);
            print "Converted date in secs is:" . $hashCodeForColor ."\n";
            my $modIdxForColor = $hashCodeForColor % scalar(@$colors);
            print "Adding color: " . @$colors[$modIdxForColor];

            #Get a unique symbol for the run
            $hashCodeForSymbol = convertDateIntoNumber($run);
            my $modIdxForSymbol = $hashCodeForSymbol % scalar(@$symbols);
            print "===========================\n";
            $runAndColor{$run}=@$colors[$modIdxForColor];
            $runAndSymbol{$run}=@$symbols[$modIdxForSymbol];
        }
		next;
	}else{
		#Data lines now
		$line =~ s/^\s+|\s+$//g;
		print "Processing $line";
		my @tempArr = split(/\s+/,$line);
		$runAndData{$run} = \@tempArr;
		print "Checking:" . $runAndData{$run} . "\n";
		$count = $count + 2;
	}
	
}
sub generateHashCodeToDetermineLineColor{
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

# Create an XYChart object of size 900 x 500 pixels, with a light blue (EEEEFF)
# background, black border, 1 pxiel 3D border effect and rounded corners
#my $c = new XYChart(1800, 800, 0xeeeeff, 0x000000, 1);
my $c = new XYChart(1800, 1000, 0xeeeeff, 0x000000, 1);
$c->setRoundedFrame();

# Set the plotarea at (55, 58) and of size 520 x 195 pixels, with white background.
# Turn on both horizontal and vertical grid lines with light grey color (0xcccccc)
#$c->setPlotArea(55, 58, 520, 195, 0xffffff, -1, -1, 0xcccccc, 0xcccccc);
#$c->setPlotArea(55, 58, 1720, 400, 0xffffff, -1, -1, 0xcccccc, 0xcccccc);
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


# Set y-axis tick density to 30 pixels. ChartDirector auto-scaling will use this as
# the guideline when putting ticks on the y-axis.
$c->yAxis()->setTickDensity(30);
# Set axis label style to 8pts Arial Bold
$c->xAxis()->setLabelStyle("timesbd.ttf", 10);
$c->yAxis()->setLabelStyle("timesbd.ttf", 8);

# Set axis line width to 2 pixels
$c->xAxis()->setWidth(2);
$c->yAxis()->setWidth(2);

# Set the labels on the x axis.
#print "Setting labels" . join("|",@labels);
$c->xAxis()->setLabels(\@labels);
$c->xAxis()->setLabelStyle("",8,TextColor, "90");

# Display 1 out of 2 labels on the x-axis.
$c->xAxis()->setLabelStep(1);
# Add a title to the x axis
$c->xAxis()->setTitle($x_axis_title);

# Add a line layer to the chart
my $layer = $c->addLineLayer2();

# Set the default line width to 2 pixels
$layer->setLineWidth(3);

# Add the three data sets to the line layer. For demo purpose, we use a dash line
# color for the last line
print "==============================================\n";
$count=0;
foreach my $runn (keys (%runAndData)){
    
	print "Finally applying color " . $runAndColor{$runn} . " for run " . $runn . "\n";
    #$layer->addDataSet($runAndData{$runn}, $runAndColor{$runn}, $runn)->setDataSymbol($perlchartdir::CircleSymbol, 9);
    $layer->addDataSet($runAndData{$runn}, $runAndColor{$runn}, $runn)->setDataSymbol($runAndSymbol{$runn}, 9);
    
    
}

# Output the chart
$c->makeChart($graphFileName);