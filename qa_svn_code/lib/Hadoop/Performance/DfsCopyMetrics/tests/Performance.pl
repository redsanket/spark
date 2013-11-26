#!/usr/bin/perl

use Net::SSH::Perl;
 
require("../lib/ReadConfigFile.pl") or die ("can't able to find the file ../lib/ReadConfigFile.pl..");

our $no_of_cluster;
our $no_of_protocols;

# Check the Arguments
if ($#ARGV != 0 ) {
    print "Usage1: $0 <ConfigFile>\n";
    exit;
}

my $config_file=$ARGV[0];
my %Config = ();
parse_config_file ($config_file, \%Config);  # Call the parse_config_file function to create the Config hash

our $no_of_cluster = $Config{'no_of_cluster'};
our $no_of_protocols = $Config{'no_of_protocols'};
our $username = $Config{'username'};
our $password = $Config{'password'};
our $source = 1;
our $dest = 1;
our $client = 1;
our $row_size =   $no_of_cluster * $no_of_protocols * ($source + $dest);
our $col_size =   3;
our $block_size =  $row_size / $no_of_protocols;
our $no_of_element_in_block = $row_size / $block_size;
our @first_col;
our @second_col;
our @third_col;
our @metric;
our @commands;
our @executionCmd;
our @clusterNames = split(',' , $Config{'clusterNames'});
our @clusterMatrix;
our @protocol = split(',',$Config{'protocols'});
our $destinationProtocol = $Config{'destinationProtocol'};
our $destinationDirectory = $Config{'destinationDirectory'};
our $sourceDirectory = $Config{'sourceDirectory'};
our $hadoopCmd = $Config{'hadoopCmd'};


fillFirstColumn();
fillSecondColumn();
fillThirdColumn();
fillMatric();
#displayArray1();
#displayArray2();
#displayArray3();
executeCommand();

sub fillFirstColumn {	
	my $count=0;
	my $cluster_size = $#clusterNames + 1;
	for($i=0; $i< $block_size ;$i++){
		my $x = $i + 1;
		fillFirstBlock( $x , $no_of_element_in_block ,$clusterNames[$count]);
		my $z = $first_col;
		$count++;
		if($count == $cluster_size ){
			$count = 0;
		}
	} 
}

sub fillSecondColumn{
	my $count = 0;
	my $cluster_size = $#clusterNames + 1;
	my $second_col_block_size = $row_size / $no_of_cluster;
	#print " $second_col_block_size \n";
	for($i=0;$i<$no_of_cluster;$i++) {
		my $x = $i + 1;
		fillSecondBlock($x , $second_col_block_size , $clusterNames[$count] , 2 );
		$count++;
	}	
}

sub fillThirdColumn{
	my $third_col_block_size = $row_size / $no_of_cluster;
	my $count = $no_of_cluster - 1;
	#print "count = $count";
	for($i=0;$i<$no_of_cluster;$i++) {
		my $x = $i + 1;
		fillThirdBlock($x , $third_col_block_size , $clusterNames[$count] );
		$count--;
	}
}

sub fillThirdBlock{
	my $which_block = shift;
	my $block_size = shift;
	my $value = shift;
	my $block_final_index = ($which_block * $block_size) - 1; 
	#print "which_block = $which_block  , block_size = $block_size   value =  $value  block_final_index = $block_final_index \n";
	my $count = 0;
	my $protocolCounter = 0;
	while($count < $block_size ){
		$third_col[$block_final_index] = "     ".$destinationProtocol.$value.$destinationDirectory;
		$block_final_index--;
		$count++;
		if($count == $block_size) {
			last;	
		}
	}
}

sub fillSecondBlock{
	my $which_block = shift;
	my $block_size = shift;
	my $value = shift;
	my $colNo = shift;
	my $block_final_index = ($which_block * $block_size) - 1; 
	#print "which_block = $which_block  , block_size = $block_size   value =  $value  block_final_index = $block_final_index \n";
	my $count = 0;
	my $protocolCounter = 0;
	while($count < $block_size ){	
		$second_col[$block_final_index] = " ,  command , ".$protocol[$protocolCounter++]."://".$value.$sourceDirectory;
		if($protocolCounter >= 3 ){
			$protocolCounter = 0;
		}
		$block_final_index--;
		$count++;
		if($count == $block_size) {
			last;	
		}
	}
}

sub fillFirstBlock {
	my $which_block = shift;
	my $block_size = shift;
	my $value = shift;
	my $block_final_index = ($which_block * $block_size) - 1; 
	my $count = 0;
	while($count < $block_size ){
		$first_col[$block_final_index] = $value;
		#my $y = $firstArray[$block_final_index];
		$block_final_index--;
		$count++;
		#print "count = $count  ,block_size = $block_size \n";
		if($count == $block_size ){		
			last;
		}
	} 
}

sub fillMatric{
	my $str1;
	my $str2;
	my $str3;
	
	for($row=0; $row< $row_size; $row++){
		for($col=0; $col < 3 ; $col++){
			if($col == 0 ){
				$metric[$row][$col] = $first_col[$row];
				$str1 = $metric[$row][$col];
			}
			if($col == 1 ){
				$metric[$row][$col] = $second_col[$row];
				$str2 = $metric[$row][$col];
			}
			if($col == 2 ){
				$metric[$row][$col] = $third_col[$row];
				$str3 = $metric[$row][$col];
			}						
		}
		$commands[$row] = $str1 . $str2 . $str3;
	}
}

sub displayArray1{	
	for($i=0;$i<$#first_col;$i++){
		print $first_col[$i] . "\n";
	}
}

sub displayArray2{	
	for($i=0;$i< $#second_col;$i++){
		print $second_col[$i] . "\n";
	}
}

sub displayArray3{	
	for($i=0;$i< $#third_col;$i++){
		print $third_col[$i] . "\n";
	}
}


sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub executeCommand {
	my $report;
	my $loginFlag = false;
	my $temp;
	open(FOUT,">Report.txt") or die "Can't create the report file..! \n";
	@executionCmd1 = sort(@commands);
	for($row=0; $row< $row_size; $row++){
		my  $itemValue = $executionCmd1[$row];
		#print "$itemValue \n";
		@str = split(',',$itemValue);
		$hostName =  trim($str[0]);
		#print length($hostName);
		#$temp1 = " /homes/lawkp ";
		$cmd = "date \"+%s\";".$str[1].";date \"+%s\"";
		$lastStr = $str[2];
		$cmd =~ s/command/$hadoopCmd/ig;
		my $ssh = Net::SSH::Perl->new($hostName);
		$ssh->login($username,$password);
		my($stdout , $stderr, $exit) = $ssh->cmd($cmd);		
		$output = $stdout;
		#print  "output = $output \n" ;
		@x = split(/\n/, $output);
		$t3 = $x[$#x] - $x[0];
	 	print "$hostName $hadoopCmd $str[2]    t1 = $x[0]  t2 = $x[$#x]  t3 = $t3  \n ";	
		$report = "$hostName $str[2]    t1 = $x[0]  t2 = $x[$#x]  t3 = $t3  \n ";
		print FOUT $report;
	}
	close(FOUT);
}
