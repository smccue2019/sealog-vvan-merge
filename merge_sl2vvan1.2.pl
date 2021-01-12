#!/usr/bin/env perl

use warnings;
use 5.010;

use Time::Local;
use File::Basename;

#====== ingest Sealog exported events csv files ===============
#=== combine separate event-relevant fields into one string ===

# Define collections of event files
my @slfiles = glob("/home/scotty/bin/CorrectAT42-12/Sealog/*_f.csv");
my @vvfiles = glob("/webdata/DAQ/at42-12/Jason/EIC/OrigVVan1/*.eic");

my %h1=(); # hash of csv fields. Keyed by timestamp. Values are 1. column header and 2. column values. 
my %h2=(); # hash of to redfines column values to be keyed by timestamp and column header. 
my %sl_evts=(); # hash of rewritten events, keyed by timestamp. 
my %nr=();

# Process all csv files into a single hash table. One file per lowering.
foreach my $slfile (@slfiles) {

	print "Ingesting $slfile\n";
	open FH, "<$slfile";

	# First line of each file lists the column headers.
	# Their order and number changes with each Jason lowering.
	my $line1 = <FH>;

        my @column_names=split(',', $line1);

	my $idx=0;
	foreach my $column_name (@column_names) {
		$column_name =~ s/[^[:print:]]//g;  # remove chars that were breaking successful keying.
       		$h1{$idx}{column_name}=$column_name; 
		$idx++;
	}

	foreach $line (<FH>) {

	  my @values = split(',', $line);
	  my $values_cnt = @values;
	  my $evt = $values[0];
	  if (($evt =~ /ROV Actions/) or ($evt =~ /HIGHLIGHTS/) or ($evt =~ /VEHICLE/) or ($evt =~ /Elevator Ops/) or ($evt =~ /NAV/) or ($evt =~ /WATCH/) or ($evt =~ /Other Samples/) or ($evt =~ /Connection Status/) or ($evt =~ /Biology Type/) or ($evt =~ /Site Interactions/) or ($evt =~ /Geologic Formations/) or ($evt =~ /Sample Type/) or ($evt =~ /Problem/)) {
		$idx=0;
		my $iso8601_value = $values[2];   # timestamp is column 3
		my $timestamp = iso8601_to_ut($iso8601_value); 
                unless ($timestamp == 0) {
		  foreach my $value (@values) {
			$column_name = $h1{$idx}{column_name};
			$h2{$timestamp}{$column_name}=$value;
			$idx++;
		  }
		  @values=();
                 }
          }
        }
	close(FH);	
}

foreach my $tskey (keys %h2) {

	$evt = $h2{$tskey}{event_value};
	
	# For better matching to virtualvan, key the resulting hash by integer unix epoch seconds.
	$tskeyi = int($tskey);
        $sl_evts{$tskeyi}{event_type}="Unknown";
        $sl_evts{$tskeyi}{was_i_used}=0;
        if ($h2{$tskey}{event_free_text}) {$h2{$tskey}{event_free_text} =~ s/"""//g;}

	if ($evt =~ /ROV Actions/)  {

                if ($h2{$tskey}{event_free_text}) {$newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});}
	        if ($h2{$tskey}{event_option_actions}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_actions});}
	        if ($h2{$tskey}{event_option_event_comment}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_event_comment});}
		$sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
		$sl_evts{$tskeyi}{event_type}="ROV Actions";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /HIGHLIGHTS/)  {
		#$HIGHLIGHTcnt++;
		unless ($h2{$tskey}{event_option_highlights}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
		  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_highlights});
		}		
		$sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="HIGHLIGHTS";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /VEHICLE/) {
		#$VEHICLEcnt++;
		unless ($h2{$tskey}{event_option_milestone}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_milestone});
		}
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="VEHICLE";
		$sl_evts{$tskeyi}{was_i_used}=0;

        } elsif ($evt =~ /Elevator Ops/) {
                # None found for cruise.
		$ELEVATORcnt++;
	} elsif ($evt =~ /NAV/) {
		#$NAVcnt++;
		unless ($h2{$tskey}{event_option_nav}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {		  
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_nav});
		}
		$sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="NAV";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /WATCH/) {
		#$WATCHcnt++;
		unless ($h2{$tskey}{event_option_new_watchstander}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_new_watchstander});
		}
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="WATCH";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Other Samples/) {
		$OTHERcnt++;
	} elsif ($evt =~ /Connection Status/) {
		#$CONNECTIONcnt++;
		unless ($h2{$tskey}{event_option_cabled_plug}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_cabled_plug});
		}                
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Connection Status";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Biology Type/) {

                if ($h2{$tskey}{event_free_text}) {$newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});}
	        if ($h2{$tskey}{event_option_type}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_type});}
	        if ($h2{$tskey}{event_option_event_comment}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_event_comment});}
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Biology Type";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Site Interactions/) {

                if ($h2{$tskey}{event_free_text}) {$newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});}
	        if ($h2{$tskey}{event_option_locations}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_locations});}
	        if ($h2{$tskey}{'event_option_dive_#/summary'}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{'event_option_dive_#/summary'});}
	        if ($h2{$tskey}{event_option_instruments}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_instruments});}
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Site Interactions";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Geologic Formations/) {
		#$GEOLOGICcnt++;
		unless ($h2{$tskey}{event_option_event_formation}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_formation});
                }
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Geologic Formations";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Sample Type/) {
		#$SAMPLEcnt++;
                if ($h2{$tskey}{event_free_text}) {$newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});}
	        if ($h2{$tskey}{event_option_type}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_type});}
	        if ($h2{$tskey}{event_option_event_comment}) {$newevt = sprintf("%s : %s", $newevt, $h2{$tskey}{event_option_event_comment});}		
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Sample Type";
		$sl_evts{$tskeyi}{was_i_used}=0;

	} elsif ($evt =~ /Problem/) {
		#$PROBLEMcnt++;
		unless ($h2{$tskey}{event_option_event_comment}) {
		  $newevt = sprintf("%s : %s", $evt, $h2{$tskey}{event_free_text});
		} else {
                  $newevt = sprintf("%s : %s %s", $evt, $h2{$tskey}{event_free_text}, $h2{$tskey}{event_option_event_comment});
                }
                $sl_evts{$tskeyi}{new_evt}=$newevt;
                $sl_evts{$tskeyi}{timestamp}=$tskey;
                $sl_evts{$tskeyi}{event_type}="Problem";
		$sl_evts{$tskeyi}{was_i_used}=0;

        } else {
	      # Probably a FREE_FORM event
              #$UNKNOWNcnt++;
              $sl_evts{$tskeyi}{new_evt}=$evt;
	      $sl_evts{$tskeyi}{timestamp}=$tskey;
              $sl_evts{$tskeyi}{event_type}="UNKNOWN";
	      $sl_evts{$tskeyi}{was_i_used}=0;
        }
	#print "$sl_evts($tskeyi}{new_evt}\n";
}

#==================================== Virtualvan ingestion =======================================
foreach $vvfile (@vvfiles) {
   $outfile = sprintf("new_%s", $vvfile);
   open INF, "< $vvfile";

   while (<INF>) {
       # Keep in mind that the split yields not a value but a phrase of form "LABEL.label=value", e.g., EIC.time=2019/06/27 04:43:27.
       # Further parsing is required to obtain a value.
       $line = $_;
       $eic_field_count=0;
       @eic_fields = split('&', $line); $eic_field_count = @eic_fields;

       given ($eic_field_count) {
	 when ($_ == 49) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering,$rnvstr,$rnvlat,$rnvlon,$rnvx,$rnvy,$rnvnsrc) = @eic_fields;
         }
         when ($_ == 43) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering) = @eic_fields;
         }
         when ($_ == 44) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering) = @eic_fields;
         }
         default {
	   $etype = "PASS";  # won't match condition for assigning hash.
         }
       }           
        if ($etype =~ m/DLG/) {
        	($label,$unix_epoch_str) = split('=', $label_secs);
		($unix_epoch_secsf, $counter) = split('\.', $unix_epoch_str);
		$unix_epoch_secs = int($unix_epoch_secsf);
		$nr{$unix_epoch_secs}{unix_epoch_secsf}=$unix_epoch_secsf;
		$nr{$unix_epoch_secs}{mod} = $mod;
		$nr{$unix_epoch_secs}{df} = $df;
		$nr{$unix_epoch_secs}{title} = $title;
		$nr{$unix_epoch_secs}{time} = $time;
		$nr{$unix_epoch_secs}{lat} = $lat;
		$nr{$unix_epoch_secs}{lon} = $lon;
		$nr{$unix_epoch_secs}{depth} = $depth;
		$nr{$unix_epoch_secs}{type} = $type;
		$nr{$unix_epoch_secs}{nsrc} = $nsrc;
		$nr{$unix_epoch_secs}{nq} = $nq;
		$nr{$unix_epoch_secs}{nd} = $nd;
		$nr{$unix_epoch_secs}{nt} = $nt;
		$nr{$unix_epoch_secs}{asrc} = $asrc;
		$nr{$unix_epoch_secs}{x} = $x;
		$nr{$unix_epoch_secs}{y} = $y;
		$nr{$unix_epoch_secs}{z} = $z;
		$nr{$unix_epoch_secs}{g} = $g;
		$nr{$unix_epoch_secs}{c} = $c;
		$nr{$unix_epoch_secs}{p} = $p;
		$nr{$unix_epoch_secs}{r} = $r;
		$nr{$unix_epoch_secs}{a} = $a;
		$nr{$unix_epoch_secs}{da} = $da;
		$nr{$unix_epoch_secs}{a2} = $a2;
		$nr{$unix_epoch_secs}{SrcN1} = $SrcN1;
		$nr{$unix_epoch_secs}{SrcD1} = $SrcD1;
                $nr{$unix_epoch_secs}{SrcN2} = $SrcN2;
                $nr{$unix_epoch_secs}{SrcD2} = $SrcD2;
                $nr{$unix_epoch_secs}{SrcN3} = $SrcN3;
                $nr{$unix_epoch_secs}{SrcD3} = $SrcD3;
                $nr{$unix_epoch_secs}{SrcN4} = $SrcN4;
                $nr{$unix_epoch_secs}{SrcD4} = $SrcD4;
		$nr{$unix_epoch_secs}{etype} = $etype;
		$nr{$unix_epoch_secs}{event} = $evt;
                $nr{$unix_epoch_secs}{MAGd} = $MAGd;
                $nr{$unix_epoch_secs}{MAGt} = $MAGt;
                $nr{$unix_epoch_secs}{MAGsrc} = $MAGsrc;
                $nr{$unix_epoch_secs}{MAGx} = $MAGx;
                $nr{$unix_epoch_secs}{MAGy} = $MAGy;
                $nr{$unix_epoch_secs}{MAGz} = $MAGz;
                $nr{$unix_epoch_secs}{MAGm} = $MAGm;
                $nr{$unix_epoch_secs}{cruiseid} = $cruiseid;
                $nr{$unix_epoch_secs}{lowering} = $lowering;
		if ($eic_field_count == 49) {
		  # Unused, but addressed for completeness.
		  $nr{$unix_epoch_secs}{orig_lat} = $lat;
		  $nr{$unix_epoch_secs}{orig_lon} = $lon;
		  $nr{$unix_epoch_secs}{orig_x} = $x;
		  $nr{$unix_epoch_secs}{orig_y} = $y;
		  $nr{$unix_epoch_secs}{orig_nsrc} = $nsrc;
                  $nr{$unix_epoch_secs}{rnvstr} = $rnvstr;
                  $nr{$unix_epoch_secs}{rnvlat} = $rnvlat;
                  $nr{$unix_epoch_secs}{rnvlon} = $rnvlon;
                  $nr{$unix_epoch_secs}{rnvx} = $rnvx;
                  $nr{$unix_epoch_secs}{rnvy} = $rnvy;
                  $nr{$unix_epoch_secs}{rnvnsrc} = $rnvnsrc;
                }
	} # if $etype
   }  # while EIC infile	
   close(INF);
} # foreach vvfile

#=========================== Merge hashes ==========================

foreach $vvts (keys %nr) {
  $vvts = int($vvts);
  $vvevt = $nr{$vvts}{event};
  $vvtsplus = $vvts+1;

if (($vvevt =~ /FREE_FORM/) or ($vvevt =~ /TEST/)) {
 # Re-use the event already in the VVan EIC record.
 # Should be irrelevant given the way the merge code is written.
 $nr{$vvts}{new_event} = $vvevt;
} else {

    if ($sl_evts{$vvts}{new_evt}) {
      $nr{$vvts}{new_event} = $sl_evts{$vvts}{new_evt};
      $sl_evts{$vvts}{was_i_used} = 1;
    } elsif ($sl_evts{$vvtsplus}{new_evt}) {
      $nr{$vvts}{new_event} = $sl_evts{$vvtsplus}{new_evt};
      $sl_evts{$vvtsplus}{was_i_used} = 1;
    } else {
      # There apparently wasn't info to fill in.
      $nr{$vvts}{new_event} = $vvevt;
    }	

#  unless ($sl_evts{$vvts}{new_evt}) {
#    # There apparently wasn't info to fill in.
#    $nr{$vvts}{new_event} = $vvevt;
#  } else {
#    $nr{$vvts}{new_event} = $sl_evts{$vvts}{new_evt};
#    $sl_evts{$vvts}{was_i_used} = 1;
#}
}
}
#===== Check to see if there were sealog events that didn't match to vvan events ======

$ROV_o=0;
$HL_o=0;
$VEH_o=0;
$NAV_o=0;
$WATCH_o=0;
$Oth_o=0;
$Conn_o=0;
$Bio_o=0;
$Site_o=0;
$Geo_o=0;
$Samp_o=0;
$Prob_o=0;
$Unk_o=0;
$matched=0;

foreach $slts (keys %sl_evts) {
  if (($sl_evts{$slts}{event_type}) and ($sl_evts{$slts}{was_i_used} == 0)) {
	if ($sl_evts{$slts}{event_type} =~ /ROV Actions/) {
	  $ROV_o++;
	  #print "Orphan ROV at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /HIGHLIGHTS/) {
	  $HL_o++;
	  #print "Orphan HIGHLIGHT at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /VEHICLE/) {
	  $VEH_o++;
	  #print "Orphan VEHICLE at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /NAV/) {
          $NAV_o++;
	  print "Orphan NAV at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /WATCH/) {
	  $WATCH_0++;
	  #print "Orphan WATCH at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Other Samples/) {
	  $Oth_o++;
	  #print "Orphan Other Samples at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Connection Status/) {
	  $Conn_o++;
	  #print "Orphan Connection Status at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Biology Type/) {
	  $Bio_o++;
	  #print "Orphan Biology Type at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Site Interactions/) {
	  $Site_o++;
	  #print "Orphan Site at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Geologic Formations/) {
	  $Geo_o++;
	  #print "Orphan Geo at $slts: $sl_evts{$slts}{new_evt}\n";
        } elsif ($sl_evts{$slts}{event_type} =~ /Sample Type/) {
	  $Samp_o++;
	  #print "Orphan Sample Type at $slts: $sl_evts{$slts}{new_evt}\n";
	} elsif ($sl_evts{$slts}{event_type} =~ /Problem/) {
	  $Prob_o++;
	  #print "Orphan Problem at $slts: $sl_evts{$slts}{new_evt}\n";
        } else {
	  $Unk_o++;
	  #print "Orphan unknown at $slts: $sl_evts{$slts}{new_evt}\n";
        }
  } elsif (($sl_evts{$slts}{event_type}) and ($sl_evts{$slts}{was_i_used} == 1)) {
     $matched++;
  } else {
     $nothing=0; 
  }
}
print "Count of Sealog ROV events not matching to Virtualvan events = $ROV_o\n";
print "Count of Sealog Highlight events not matching to Virtualvan events = $HL_o\n";
print "Count of Sealog VEHICLE events not matching to Virtualvan events = $VEH_o\n";
print "Count of Sealog NAV events not matching to Virtualvan events = $NAV_o\n";
print "Count of Sealog WATCH events not matching to Virtualvan events = $WATCH_o\n";
print "Count of Sealog Other Sample events not matching to Virtualvan events = $Oth_o\n";
print "Count of Sealog Connection events not matching to Virtualvan events = $Conn_o\n";
print "Count of Sealog Biologic events not matching to Virtualvan events = $Bio_o\n";
print "Count of Sealog Site events not matching to Virtualvan events = $Site_o\n";
print "Count of Sealog Geo events not matching to Virtualvan events = $Geo_o\n";
print "Count of Sealog Sample Type events not matching to Virtualvan events = $Samp_o\n";
print "Count of Sealog Problem events not matching to Virtualvan events = $Prob_o\n";
print "Count of Sealog Unknown events not matching to Virtualvan events = $Unk_o\n";
print "Count of Sealog events matched to Virtualvan events = $matched\n";
#=============== Merged hash tied to files and write out result =======================

foreach $vvfile (@vvfiles) {
   ($fname,$fpth,$fssuffix)= fileparse($vvfile, ".eic");
   $outfile = sprintf("/home/scotty/bin/CorrectAT42-12/Output/%s_.eic", $fname);
   print "Printing to $outfile\n";
   open INF, "< $vvfile";
   open OUTF, "> $outfile";

   my %or=();

   while (<INF>) {
       $line = $_;
       $eic_field_count=0;
       @eic_fields = split('&', $line); $eic_field_count = @eic_fields;

       # Keep in mind that the split yields not a value but a phrase of form "LABEL.label=value", e.g., EIC.time=2019/06/27 04:43:27.
       # Further parsing is required to obtain a value.
       given ($eic_field_count) {
	 when ($_ == 49) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering,$rnvstr,$rnvlat,$rnvlon,$rnvx,$rnvy,$rnvnsrc) = @eic_fields;
         }
         when ($_ == 43) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering) = @eic_fields;
         }
         when ($_ == 44) {
           ($label_secs, $mod,$df,$title,$time,$lat,$lon,$depth,$type,$nsrc,$nq,$nd,$nt,$asrc,$x,$y,$z,$g,$c,$p,$r,$a,$da,$a2,$SrcN1,$SrcD1,$SrcN2,$SrcD2,$SrcN3,$SrcD3,$SrcN4,$SrcD4,$etype,$evt,$MAGd,$MAGt,$MAGsrc,$MAGx,$MAGy,$MAGz,$MAGm,$cruiseid,$lowering) = @eic_fields;
         }
         default {
	   $etype = "PASS";  # won't match condition for assigning hash.
         }
       }    

       unless ($etype =~ m/DLG/) {
	  chomp($line);
	  print OUTF "$line\n";
       } else {
          ($label,$unix_epoch_str) = split('=', $label_secs);
          ($unix_epoch_secsf, $counter) = split('\.', $unix_epoch_str);
          $ut = int($unix_epoch_secsf);
          unless ($ut) {
            print "NO TIMESTAMP FOUND: $line";
          } else {
	      $or{$ut}{label_secs}=$label_secs;
              $or{$ut}{unix_epoch_secsf}=$unix_epoch_secsf;
              $or{$ut}{mod} = $mod;
              $or{$ut}{df} = $df;
              $or{$ut}{title} = $title;
              $or{$ut}{time} = $time;
              $or{$ut}{lat} = $lat;
              $or{$ut}{lon} = $lon;
              $or{$ut}{depth} = $depth;
              $or{$ut}{type} = $type;
              $or{$ut}{nsrc} = $nsrc;
              $or{$ut}{nq} = $nq;
              $or{$ut}{nd} = $nd;
              $or{$ut}{nt} = $nt;
              $or{$ut}{asrc} = $asrc;
              $or{$ut}{x} = $x;
              $or{$ut}{y} = $y;
              $or{$ut}{z} = $z;
              $or{$ut}{g} = $g;
              $or{$ut}{c} = $c;
              $or{$ut}{p} = $p;
              $or{$ut}{r} = $r;
              $or{$ut}{a} = $a;
              $or{$ut}{da} = $da;
              $or{$ut}{a2} = $a2;
              $or{$ut}{SrcN1} = $SrcN1;
              $or{$ut}{SrcD1} = $SrcD1;
              $or{$ut}{SrcN2} = $SrcN2;
              $or{$ut}{SrcD2} = $SrcD2;
              $or{$ut}{SrcN3} = $SrcN3;
              $or{$ut}{SrcD3} = $SrcD3;
              $or{$ut}{SrcN4} = $SrcN4;
              $or{$ut}{SrcD4} = $SrcD4;
              $or{$ut}{etype} = $etype;
              $or{$ut}{event} = $evt;
	      $or{$ut}{MAGd} = $MAGd;
              $or{$ut}{MAGt} = $MAGt;
              $or{$ut}{MAGsrc} = $MAGsrc;
              $or{$ut}{MAGx} = $MAGx;
              $or{$ut}{MAGy} = $MAGy;
              $or{$ut}{MAGz} = $MAGz;
              $or{$ut}{MAGm} = $MAGm;
              $or{$ut}{cruiseid} = $cruiseid;
              $or{$ut}{lowering} = $lowering;
	      if ($eic_field_count == 49) {
                ($toss,$latval) = split('=',$lat); $or{$ut}{orig_lat} = sprintf("EIC.orig_lat=$latval");
		($toss,$lonval) = split('=',$lon); $or{$ut}{orig_lon} = sprintf("EIC.orig_lon=$lonval");
		($toss,$xval) = split('=', $x); $or{$ut}{orig_x} = sprintf("EIC.orig_x=$xval");
		($toss,$yval) = split('=', $y); $or{$ut}{orig_y} = sprintf("EIC.orig_y=$yval");
		($toss,$nsrcval) = split('=', $nsrc); $or{$ut}{orig_nsrc} = sprintf("EIC.orig_y=$nsrcval");
                $or{$ut}{rnvstr} = $rnvstr;
                $or{$ut}{rnvlat} = $rnvlat;
                $or{$ut}{rnvlon} = $rnvlon;
                $or{$ut}{rnvx} = $rnvx;
                $or{$ut}{rnvy} = $rnvy;
                $or{$ut}{rnvnsrc} = chomp($rnvnsrc);
              } # if eic_field_count == 49
          }

         unless ($nr{$ut}{new_event} =~ /DAQ.evt/) {
	  $nr{$ut}{new_event} = sprintf("DAQ.evt=%s", $nr{$ut}{new_event});
         }

         if ($eic_field_count == 49) {
           $outstr=sprintf("%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&\n",$or{$ut}{label_secs},$or{$ut}{mod},$or{$ut}{df},$or{$ut}{title},$or{$ut}{time},$or{$ut}{orig_lat},$or{$ut}{orig_lon},$or{$ut}{depth},$or{$ut}{type},$or{$ut}{orig_nsrc},$or{$ut}{nq},$or{$ut}{nd},$or{$ut}{nt},$or{$ut}{asrc},$or{$ut}{orig_x},$or{$ut}{orig_y},$or{$ut}{z},$or{$ut}{g},$or{$ut}{c},$or{$ut}{p},$or{$ut}{r},$or{$ut}{a},$or{$ut}{da},$or{$ut}{a2},$or{$ut}{SrcN1},$or{$ut}{SrcD1},$or{$ut}{SrcN2},$or{$ut}{SrcD2},$or{$ut}{SrcN3},$or{$ut}{SrcD3},$or{$ut}{SrcN4},$or{$ut}{SrcD4},$or{$ut}{etype},$nr{$ut}{new_event},$or{$ut}{MAGd},$or{$ut}{MAGt},$or{$ut}{MAGsrc},$or{$ut}{MAGx},$or{$ut}{MAGy},$or{$ut}{MAGz},$or{$ut}{MAGm},$or{$ut}{cruiseid},$or{$ut}{lowering},$or{$ut}{rnvstr},$or{$ut}{rnvlat},$or{$ut}{rnvlon},$or{$ut}{rnvx},$or{$ut}{rnvnsrc});
         } else {
           $outstr=sprintf("%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&%s&\n",$or{$ut}{label_secs},$or{$ut}{mod},$or{$ut}{df},$or{$ut}{title},$or{$ut}{time},$or{$ut}{lat},$or{$ut}{lon},$or{$ut}{depth},$or{$ut}{type},$or{$ut}{nsrc},$or{$ut}{nq},$or{$ut}{nd},$or{$ut}{nt},$or{$ut}{asrc},$or{$ut}{x},$or{$ut}{y},$or{$ut}{z},$or{$ut}{g},$or{$ut}{c},$or{$ut}{p},$or{$ut}{r},$or{$ut}{a},$or{$ut}{da},$or{$ut}{a2},$or{$ut}{SrcN1},$or{$ut}{SrcD1},$or{$ut}{SrcN2},$or{$ut}{SrcD2},$or{$ut}{SrcN3},$or{$ut}{SrcD3},$or{$ut}{SrcN4},$or{$ut}{SrcD4},$or{$ut}{etype},$nr{$ut}{new_event},$or{$ut}{MAGd},$or{$ut}{MAGt},$or{$ut}{MAGsrc},$or{$ut}{MAGx},$or{$ut}{MAGy},$or{$ut}{MAGz},$or{$ut}{MAGm},$or{$ut}{cruiseid},$or{$ut}{lowering});
         }
         print OUTF "$outstr";
      }  # unless-else
   }   # while INF
   close(INF);
   close(OUTF);
}  # foreach EIC
################# Subroutines #####################

sub iso8601_to_ut {
        my $iso8601 = $_[0];

        if ($iso8601 =~ m{
                        (\d{4})
                        -
                        (\d{2})
                        -
                        (\d{2})
                        T
                        (\d{2})
                        :
                        (\d{2})
                        :
                        (\d{2}.\d{3})
                        Z
                      }x ) {

        $yr = $1;
        $mo = $2; $mo -= 1;
        $dy = $3;
        $hr = $4;
        $min = $5;
        $sec = $6;

        $unix_seconds = timegm($sec,$min,$hr,$dy,$mo,$yr);
        return ($unix_seconds);
        } else {
        return (0);
        }
}



