#!/usr/bin/perl
###################################################################################
#                                                                                 
# Description : Converts IP2Location LITE DB1 into MaxMind GeoLite2 Country data or
#               Converts IP2Location LITE DB11 into MaxMind GeoLite2 City data
#                                                                                 
# Usages      : perl convert.pl IP2LOCATION-LITE-DB1.CSV
#               perl convert.pl IP2LOCATION-LITE-DB11.CSV
#                                                                                 
# Note        : Continent, geonames id, confidence, accuracy radius, metro code,
#               time zone, registered country, and represented country fields are 
#               not supported. Only English names are supported at present. 
#
# Author      : Антон Владимиревич <anton.vladimir@europe.com>
#
# Disclaimer  : IP2Location and Maxmind are trademark of respective owners.
#               The IP2Location and GeoLite2 data structure is based on the 
#               public specification published by the respective owners.
#
###################################################################################
use strict;
use Socket;

if (($#ARGV + 1) != 1) {
	print "Usage: perl convert.pl <IP2Location LITE DB1 or DB11 CSV file>\n";
	exit();
}

my $filename = $ARGV[0];
my $filename2 = $filename . ".MMDB";

my %sortbylength;
my %btree;
my @data;
my @cidrdata;
my $csvdata = "";
my @csvdataarray;
my %tokens = ("country" => 0, "iso_code" => 0, "names" => 0, "en" => 0, "-" => 0);
my %tokens2 = ("city" => 0, "location" => 0, "postal" => 0, "latitude" => 0, "longitude" => 0, "code" => 0, "subdivisions" => 0);

my %countryoffset;
my %cityoffset;
my %countries;
my %cities;
my %latlongs;
my $filetype = "";
my $datastartmarker = &print_byte(0) x 16;
my $datastartmarkerlength = length($datastartmarker);

{
	open IN, "<$filename" or die;
	local $/ = undef;
	$csvdata = <IN>;
	close IN;
}

@csvdataarray = split(/[\r\n]+/, $csvdata);
$csvdata = "";

foreach my $csvline (@csvdataarray) {
	my $therest = '';
	my @array = &splitcsv($csvline);
	if ($filetype eq '') {
		if (scalar(@array) == 10)	{
			$filetype = "city";
		}
		else {
			$filetype = "country";
		}
	}
	if ($filetype eq "city") {
		pop(@array);
		$tokens{$array[2]} = 0;
		$tokens{$array[3]} = 0;
		$tokens{$array[4]} = 0;
		$tokens{$array[5]} = 0;
		$latlongs{$array[6]} = 0;
		$latlongs{$array[7]} = 0;
		$tokens{$array[8]} = 0;
		
		$cities{$array[2] . "|" . $array[3] . "|" . $array[4] . "|" . $array[5] . "|" . $array[6] . "|" . $array[7] . "|" . $array[8]} = 0;
		$therest = $array[2] . "|" . $array[4] . "|" . $array[5] . "|" . $array[6] . "|" . $array[7] . "|" . $array[8];
	}	elsif ($filetype eq "country") {
		$countries{$array[2]} = $array[3];
		$therest = $array[2];
	}
	my $fromip = &no2ip($array[0]);
	my $toip = &no2ip($array[1]);
	my @ar = &range2cidr("$fromip-$toip");
	foreach my $a (sort ipnumber_sort @ar) {
		push (@cidrdata, '"' . $a . '",' . $therest);
	}
}

undef(@csvdataarray);

sub range2cidr {
	my @r = @_;
	my $i;
	my @c;

	for ($i=0; $i <= $#r; $i++)	{
		$r[$i] =~ s/\s//g;
		if ($r[$i] =~ /\//)	{
			push @c, $r[$i];
			next;
		}
		
		$r[$i]="$r[$i]-$r[$i]" unless $r[$i] =~ /(.*)-(.*)/;
		$r[$i] =~ /(.*)-(.*)/;
		
		my ($a,$b)=($1,$2);
		my @a=split(/\.+/, $a);
		my @b=split(/\.+/, $b);
		
		return unless $#a == $#b;
		my @cc=_range2cidr(\@a, \@b);
		
		while ($#cc >= 0)	{
			$a=shift @cc;
			$b=shift @cc;
			push @c, "$a/$b";
		}
	}
	
	return @c unless(1==@r && 1==@c && !wantarray());
	return $c[0];
}

sub _range2cidr {
	my $a=shift;
	my $b=shift;

	my @a=@$a;
	my @b=@$b;

	$a=shift @a;
	$b=shift @b;

	return _range2cidr8($a, $b) if $#a < 0;

	die unless $a >= 0 && $a <= 255 && $a =~ /^[0-9]+$/;
	die unless $b >= 0 && $b <= 255 && $b =~ /^[0-9]+$/ && $b >= $a;

	my @c;

	if ($a == $b)	{
		my @cc= _range2cidr(\@a, \@b);
		
		while ($#cc >= 0)	{
			my $c=shift @cc;
	    push @c, "$a.$c";
	    $c=shift @cc;
	    push @c, $c+8;
		}
		return @c;
	}

	my $start0=1;
	my $end255=1;

	grep { $start0=0 unless $_ == 0; } @a;
	grep { $end255=0 unless $_ == 255; } @b;

	if (! $start0) {
		my @bcopy=@b;
		grep { $_=255 } @bcopy;
		my @cc= _range2cidr(\@a, \@bcopy);
		while ($#cc >= 0)	{
			my $c=shift @cc;
			push @c, "$a.$c";
			$c=shift @cc;
			push @c, $c + 8;
		}
		++$a;
  }

	if (! $end255) {
		my @acopy=@a;
		grep { $_=0 } @acopy;
		my @cc= _range2cidr(\@acopy, \@b);
		while ($#cc >= 0)	{
			my $c=shift @cc;
			push @c, "$b.$c";
			$c=shift @cc;
			push @c, $c + 8;
		}
		--$b;
	}

	if ($a <= $b) {
		grep { $_=0 } @a;
		my $pfix=join(".", @a);
		my @cc= _range2cidr8($a, $b);
		while ($#cc >= 0)	{
			my $c=shift @cc;
			push @c, "$c.$pfix";
			$c=shift @cc;
	    push @c, $c;
		}
	}
	return @c;
}

sub _range2cidr8
{
	my @c;
	my @r = @_;
	
	while ($#r >= 0) {
		my $a=shift @r;
		my $b=shift @r;
		
		die unless $a >= 0 && $a <= 255 && $a =~ /^[0-9]+$/;
		die unless $b >= 0 && $b <= 255 && $b =~ /^[0-9]+$/ && $b >= $a;
		
		++$b;
		
		while ($a < $b)	{
			my $i=0;
			my $n=1;
			
			while ( ($n & $a) == 0)	{
				++$i;
				$n <<= 1;
				last if $i >= 8;
			}
			while ($i && $n + $a > $b)	{
				--$i;
				$n >>= 1;
			}
			push @c, $a;
			push @c, 8-$i;
			$a += $n;
		}
	}
	return @c;
}

sub no2ip {
	my $no = shift(@_);
	return inet_ntoa(pack("N", $no));
}

sub ip2no {
	my $ip = shift(@_);
	return unpack("N",inet_aton($ip));
}

sub ipnumber_sort {
	my @a_val = split('/', $a);
	my @b_val = split('/', $b);
	return &ip2no($a_val[0]) <=> &ip2no($b_val[0]);
}

sub splitcsv() {
	my $line = shift (@_);
	my $sep = ',';
	return () unless $line;
	my @cells;
	$line =~ s/\r?\n$//;
	my $re = qr/(?:^|$sep)(?:\"([^\"]*)\"|([^$sep]*))/;
	while($line =~ /$re/g) {
		my $value = defined $1 ? $1 : $2;
		push @cells, (defined $value ? $value : '');
	}
	return @cells;
}

while (my $line = shift(@cidrdata)) {
	if ($line =~ /^"([\d\.]+)\/(\d+)",(.*)$/)	{
		my $ip = $1;
		my $cidr = $2;
		$line = $3;
		
		my @iparr = split(/\./, $ip);
		my @binary = map { sprintf("%08b", $_) } @iparr;
		my $binarystr = join("", @binary);
		my $binarystrcidr = substr($binarystr, 0, $cidr);
		
		$sortbylength{"GG" . $binarystrcidr} = $line;
	}
}

undef(@cidrdata);

my $datasection = "";
my $stringtype = 2 << 5;
my $maptype = 7 << 5;
my $pointertype = 1 << 5;
my $uint16type = 5 << 5;
my $uint32type = 6 << 5;
my $uint64type = 9 - 7;
my $arraytype = 11 - 7;
my $extendedtype = 0;
my $doubletype = 3 << 5;

if ($filetype eq "city") {
	my %newHash = (%tokens, %tokens2);
	%tokens = %newHash;
}

foreach my $token (sort keys(%tokens)) {
	$tokens{$token} = length($datasection);
	my $tokenlength = length($token);
	my $controlbyte = $stringtype | $tokenlength;
	
	$datasection .= &print_byte($controlbyte) . &print_str($token, $tokenlength);
}

foreach my $latlong (sort keys(%latlongs)) {
	$latlongs{$latlong} = length($datasection);
	my $controlbyte = $doubletype | 8;
	
	$datasection .= &print_byte($controlbyte) . &print_double($latlong);
}

if ($filetype eq "country") {
	foreach my $countrycode (sort keys(%countries))	{
		$countryoffset{$countrycode} = length($datasection);
		
		my $controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"country"});
		
		$controlbyte = $maptype | 2;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"iso_code"});
		
		my $tokenlength = length($countrycode);
		$controlbyte = $stringtype | $tokenlength;
		$datasection .= &print_byte($controlbyte) . &print_str($countrycode, $tokenlength);
		$datasection .= &print_pointer($tokens{"names"});
		
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"en"});
		
		my $countryname = $countries{$countrycode};
		$tokenlength = length($countryname);
		$controlbyte = $stringtype | $tokenlength;
		$datasection .= &print_byte($controlbyte) . &print_str($countryname, $tokenlength);
	}
	undef(%countries);
}
elsif ($filetype eq "city")
{
	foreach my $stuff (sort keys(%cities))	{
		my @array = split(/\|/, $stuff);
		my $countrycode = $array[0];
		my $countryname = $array[1];
		my $statename = $array[2];
		my $cityname = $array[3];
		my $latitude = $array[4];
		my $longitude = $array[5];
		my $postcode = $array[6];
		
		$cityoffset{$countrycode . "|" . $statename . "|" . $cityname . "|" . $latitude . "|" . $longitude . "|" . $postcode} = length($datasection);
		
		my $controlbyte = $maptype | 5;
		$datasection .= &print_byte($controlbyte);
		
		$datasection .= &print_pointer($tokens{"city"});
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"names"});
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"en"});
		$datasection .= &print_pointer($tokens{$cityname});
		
		$datasection .= &print_pointer($tokens{"country"});
		$controlbyte = $maptype | 2;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"iso_code"});
		$datasection .= &print_pointer($tokens{$countrycode});
		$datasection .= &print_pointer($tokens{"names"});
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"en"});
		$datasection .= &print_pointer($tokens{$countryname});
		
		$datasection .= &print_pointer($tokens{"location"});
		$controlbyte = $maptype | 2;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"latitude"});
		$datasection .= &print_pointer($latlongs{$latitude});
		$datasection .= &print_pointer($tokens{"longitude"});
		$datasection .= &print_pointer($latlongs{$longitude});
		
		$datasection .= &print_pointer($tokens{"postal"});
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"code"});
		$datasection .= &print_pointer($tokens{$postcode});
		
		$datasection .= &print_pointer($tokens{"subdivisions"});
		my $myint = 1;
		$controlbyte = $extendedtype | $myint;
		my $typebyte = $arraytype;
		$datasection .= &print_byte($controlbyte) . &print_byte($typebyte);
		$controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"names"});
		my $controlbyte = $maptype | 1;
		$datasection .= &print_byte($controlbyte);
		$datasection .= &print_pointer($tokens{"en"});
		$datasection .= &print_pointer($tokens{$statename});
	}
	undef(%cities);
}

foreach my $binarystrcidr (sort keys(%sortbylength))
{
	my $tmp = $binarystrcidr;
	$tmp =~ s/GG//;
	my @myarr = split(//, $tmp);

	my $code = '$btree';
	foreach (@myarr) {
		$code .= '{"x' . $_ . '"}';
	}
	$code .= ' = "' . $sortbylength{$binarystrcidr} . '";';

	eval($code);
	warn $@ if $@;
}

undef(%sortbylength);

travtree(\%btree, 0, '');

undef(%btree);

my $totalnodes = 0;
my @offsetnodes;

foreach my $x (0..$#data) {
	my $nodes = @{$data[$x]};
	$totalnodes += $nodes;
	$offsetnodes[$x] = $totalnodes;
}

open OUT, ">$filename2" or die;
binmode OUT;

foreach my $x (0..$#data)
{
	my @datalevel = @{$data[$x]};
	
	foreach my $y (0..$#datalevel) {
		my $nodedata = $datalevel[$y];
		
		if ($nodedata =~ /^(.*)\#(.*)$/)		{
			my $left = $1;
			my $right = $2;
			my $leftdata = 0;
			my $rightdata = 0;
			
			if ($left =~ /^\d+$/)	{
				$left += $offsetnodes[$x];
				$leftdata = $left;
			}	else {
				if ($filetype eq 'country') {
					$leftdata = $countryoffset{$left} + $datastartmarkerlength + $totalnodes;
				}	elsif ($filetype eq 'city')	{
					$leftdata = $cityoffset{$left} + $datastartmarkerlength + $totalnodes;
				}
			}
			if ($right =~ /^\d+$/) {
				$right += $offsetnodes[$x];
				$rightdata = $right;
			}	else {
				if ($filetype eq 'country')	{
					$rightdata = $countryoffset{$right} + $datastartmarkerlength + $totalnodes;
				}	elsif ($filetype eq 'city')	{
					$rightdata = $cityoffset{$right} + $datastartmarkerlength + $totalnodes;
				}
			}
			print OUT &print_node($leftdata, $rightdata);
		}
	}
}

undef(@data);

print OUT $datastartmarker;
print OUT $datasection;
print OUT &print_hex("ABCDEF4D61784D696E642E636F6D");
my $controlbyte = $maptype | 9;
print OUT &print_byte($controlbyte);
my $field = "binary_format_major_version";
my $fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
my $myint = 2;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $uint16type | $intbytes;
print OUT &print_byte($controlbyte) . $myint;
$field = "binary_format_minor_version";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
my $myint = 0;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $uint16type | $intbytes;
print OUT &print_byte($controlbyte) . $myint;
$field = "build_epoch";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$myint = time;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $extendedtype | $intbytes;
my $typebyte = $uint64type;
print OUT &print_byte($controlbyte) . &print_byte($typebyte) . $myint;
$field = "database_type";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$field = ($filetype eq 'country') ? "IP2LITE-Country" : "IP2LITE-City";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$field = "description";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
my $controlbyte = $maptype | 1;
print OUT &print_byte($controlbyte);
$field = "en";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$field = ($filetype eq 'country') ? "IP2LITE-Country database" : "IP2LITE-City database";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$field = "ip_version";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
my $myint = 4;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $uint16type | $intbytes;
print OUT &print_byte($controlbyte) . $myint;
$field = "languages";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$myint = 1;
$controlbyte = $extendedtype | $myint;
my $typebyte = $arraytype;
print OUT &print_byte($controlbyte) . &print_byte($typebyte);
$field = "en";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$field = "node_count";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$myint = $totalnodes;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $uint32type | $intbytes;
print OUT &print_byte($controlbyte) . $myint;
$field = "record_size";
$fieldlength = length($field);
$controlbyte = $stringtype | $fieldlength;
print OUT &print_byte($controlbyte) . &print_str($field, $fieldlength);
$myint = ($filetype eq 'country') ? 24 : 28;
$myint = &print_uint($myint);
my $intbytes = length($myint);
$controlbyte = $uint32type | $intbytes;
print OUT &print_byte($controlbyte) . $myint;

close OUT;

print "You have successfully converted $filename to $filename2.\n";
print "You can now use the $filename2 with any MaxMind API which supports the GeoLite2 format.\n";

sub travtree {
	my ($hash, $level, $trace) = @_;
	my $leftval = -1;
	my $rightval = -1;
	my $leftleaf = 0;
	my $rightleaf = 0;
	while (my ($key, $value) = each %$hash)	{
		my $key2 = $key;
		my $trace2 = $trace . $key2;
		
		if ('SCALAR' eq ref(\$value)) {
			if ($key eq 'x0')	{
				$leftval = $value;
				$leftleaf = 1;
			}	elsif ($key eq 'x1') {
				$rightval = $value;
				$rightleaf = 1;
			}
		} elsif ('REF' eq ref(\$value))	{
			my $tmp = &travtree(\%{$value}, $level + 1, $trace2);
			
			if ($key eq 'x0')	{
				$leftval = $tmp;
			}	elsif ($key eq 'x1')	{
				$rightval = $tmp;
			}
		}
	}

	my $ownoffset = 0;
	if (defined($data[$level]))	{
		$ownoffset = @{$data[$level]};
	}	else {
		$data[$level] = ();
	}
	
	$data[$level][$ownoffset] = $leftval . "#" . $rightval;
	return $ownoffset;
}

sub print_double {
	my $num = shift;
	my $s = pack('d>', $num);
	return $s;
}

sub print_uint {
	my $num = shift;
	my $s = "";
	while ($num > 0) {
		my $num2 = $num & 0xFF;
		$s = &print_byte($num2) . $s;
		$num = $num >> 8;
	}
	return $s;
}

sub print_str {
	my $value = shift;
	my $x = shift;
	my $s = pack('A' . $x, $value);
	return $s;
}

sub print_byte {
	my $num = shift;
	my $s = pack('C', $num);
	return $s;
}

sub print_hex {
	my $num = shift;
	my $s = pack('H*', $num);
	return $s;
}

sub print_node {
	my $leftdata = shift;
	my $rightdata = shift;
	my @mybytes;
	
	if ($filetype eq 'country')	{
		my @leftbytes = &get_byte_array($leftdata, 3);
		my @rightbytes = &get_byte_array($rightdata, 3);
		@mybytes = (@leftbytes, @rightbytes);
	}	elsif ($filetype eq 'city')	{
		my @leftbytes = &get_byte_array($leftdata, 4);
		my @rightbytes = &get_byte_array($rightdata, 4);
		my $midbyte = ($leftbytes[0] << 4) ^ $rightbytes[0];
		shift(@leftbytes);
		shift(@rightbytes);
		push(@leftbytes, $midbyte);
		@mybytes = (@leftbytes, @rightbytes);
	}
	
	my $s = "";
	foreach (@mybytes) {
		$s .= &print_byte($_);
	}
	return $s;
}

sub print_pointer
{
	my $num = shift;
	my $pointersize = -1;
	my $threebits = 0;
	my @balance;
	
	if ($num <= 2047)	{
		$pointersize = 0;
		$threebits = $num >> 8;
		@balance = &get_byte_array($num, 1);
	}	elsif ($num <= 526335) {
		$pointersize = 1;
		$num = $num - 2048;
		$threebits = $num >> 16;
		@balance = &get_byte_array($num, 2);
	}	elsif ($num <= 134744063)	{
		$pointersize = 2;
		$num = $num - 526336;
		$threebits = $num >> 24;
		@balance = &get_byte_array($num, 3);
	}	elsif ($num <= 4294967295) {
		$pointersize = 3;
		$threebits = 0;
		@balance = &get_byte_array($num, 4);
	} else {
		die "Pointer value too large.\n";
	}
	
	$pointersize = $pointersize << 3;
	
	my $controlbyte = $pointertype | $pointersize | $threebits;
	
	my $s = &print_byte($controlbyte);
	
	foreach (@balance)	{
		$s .= &print_byte($_);
	}
	
	return $s;
}

sub get_byte_array {
	my $num = shift;
	my $bytes = shift;
	my @bytesarr;
	my $tmp;
	
	foreach (1..$bytes) {
		$tmp = $num & 0xFF;
		$num = $num >> 8;
		unshift(@bytesarr, $tmp);
	}
	return @bytesarr;
}
