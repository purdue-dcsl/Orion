#!/usr/bin/perl -w

# An extension of my parse_blast.pl script - giving info on "gap free blocks"
# - used primarily as part of the "FragBlast.pl" wrapper script.
#
# This script parses the output of BLASTN (through stdin or via '-in file' ) into 
# a condensed format displaying one match per line. An example of the format is:
# IDB1093,17073,10610..10776|IDB46,6975,3089..3251|-1,6e-48,149/167,4   [the gap free blocks info]
# where the first three fields describe the subject, its length and 
# the position of the hit; the next three fields describe the same 
# parameters for the query sequence. The next four fields are: the sense,
# the blast expectation, the identities and the gaps. We'll get to the
# "[the gap free blocks info]" shortly.
#
# An example of the use of this script might be:
# blastall -p blastn -d file1.fna -i file2.fna | ~/perl/fb_ParseBlastN.pl > out.parsed
#
# This usage gives the same output as for my standard parse_blast.pl script
# - that is, no "[the gap free blocks info]" data.
#
# Gap and mismatch information: This can be obtained by using the '-fs' switch.
# This information is given as a second, white space separated, field for each 
# match. By way of example, the output |59..95:774..810,76|2|97..102:811..816,98,99|
# states that region 59..95 of the query sequence matches, without gaps, to 774..810
# of the subject sequence, with a single mismatch at query position 76. There
# is a 2 nt gap, before the next gapless block of match between query 97..102
# and subject 811..816, containing mismatches at query positions 98 & 99.
#
# There is a alternative option '-fs_ends #' which produces mismatch and gap 
# information as above but only for match against the first and last # nts of the 
# query sequence. This option is implemented for use by the FragBlast script.
# In this case the format is a little different, with the information for 
# the two ends is separated by one or more 'X's (if there is any info).
#
# This script also, when in -fs (or fs_ends) mode, outputs the occasional
# line of the form "lambda = 1.37, K = 0.711, H = 1.31". This information
# is used by FragBlast for calculation of E-values.
#
# Warning: This script takes the names of the sequences to be the first 
# word (/^>(\S+)/) of the description in the blast output. 
# This is generally a unique identifier. I always check / set them up myself. 
# You can set the flag 'DISPLAY_FULL_TAG' to '1', but then the parsed output
# can end up looking fairly nasty. In any case sequence tags that are not 
# -simple- tags (eg pipe separated goop) will tend to create a mess of the
# output (i.e. the ease with which it can be read and parsed).
# 
# Warning: the output of blast -is not- always perfect / consistent / machine 
# readable (or at least has not historically been so). While such problems 
# seem to have been mostly fixed, occasional hiccups might still arise. 
# This script produces a stderr output if it encounters any problems, 
# and may well die in such cases - so it is important to keep track of
# the stderr output - i.e. make sure there wasn't any. You can redirect the
# stderr into a file with the command line option "-err file.err".
#
# Apology: This script is not the most beautiful piece of Perl in the world.
# It was initially developed in a manic phase of my PhD studies, and tested
# quite throughly, but at that time my scripting 'clarity and cleanliness' 
# was still on a steep learning curve. I've now done a substantial cleanup,
# but could still do a lot more to make it readable. Maybe it'll happen
# one day - particularly if I become aware it is being used. If you have 
# specific comments, bug reports, improvements etc, please let me know.
# 
# DISCLAIMER:  This script is made available in the hope that it might be useful,
#              but without any warranty of any sort. If it's important, test it
#              and validate it yourself!
# 
# Francis Clark - 14 March 2006 - fc at acmc uq edu au 
#
# Issues: - should really fold into main parser.
#         - could/should have options to get more info, such as the nucleotides
#           at mismatch positions.
#         - Only works for blastn - because I only envisage using FragBlast for
#           nucleotides; fold-in to main parser would resolve this.


$DISPLAY_FULL_TAG = 0;
$BL2SEQ           = 0;
$FINESTRUCT       = 0;
$FS_ENDS          = 0;

$IN_FILE  = '';
$OUT_FILE = '';
$ERR_FILE = '';

$TEST_RUN = 0;     # If on (1); do only the first 10 lines and then exit normally

$BUFFER   = 40;     # buffer output this many lines at a time
$buf_str  = '';
$buf_cnt  = 0;

     $IN_FILE    = $ARGV[0]; 
    $OUT_FILE   = $ARGV[1];
  
    $FINESTRUCT = 1;  

if( $ERR_FILE ) { open STDERR, ">$ERR_FILE"  or die "ERROR redirecting stderr: >$ERR_FILE\n"; }
if( $OUT_FILE ) { open STDOUT, ">$OUT_FILE"  or die "ERROR redirecting stderr: >$OUT_FILE\n"; }
if( $IN_FILE  ) { open STDIN,   "$IN_FILE"   or die "ERROR redirecting stdin: $IN_FILE\n";    }

$out_byte_count = 0;
$pre_param_str  = '';
$param_str      = '';


while( <STDIN> )
{
    # Read query details

X:  if( /^Query=\s*((\S+).*)$/ )
    {
        $query_dest = $1;
        $query      = $2;
        $query_lng  = '';
        
        do
        {
            $_ = <STDIN>;  tr/,//d;
            if( /\s+\((\d+)\s+letters\)/ ) {  $query_lng = $1;  }
            elsif( /^$/ )
            { 
                warn "WARNING: [$query_dest]: parse failure looking for query length.\n";
                $query_lng = 'error';
            }
            else       # dealing with a multi-line query description
            {
                chomp;
                $query_dest .= $_;
            }
        } while( not $query_lng );
        
        if( $DISPLAY_FULL_TAG ) { $query = $query_dest; }
    }
    
    
    # Read subject details
    
    if( /^>((\S+).*)/ )
    {
        $subject_dest = $1;
        $subject      = $2;
        $subject_lng  = '';
        
        do
        {
            $_ = <STDIN>;  tr/,//d;
            if( /\s+Length\s*=\s*(\d+)/ ) {  $subject_lng = $1;  }
            elsif( /^$/ )
            { 
                warn "WARNING: [$query/$subject_dest]: parse failure looking for subject length.\n"; 
                $subject_lng = 'error';
            }
            else      # dealing with a multi-line query description
            {
                chomp;
                $subject_dest .= $_;
            }
        } while( not $subject_lng );
        
        if( $DISPLAY_FULL_TAG ) { $subject = $subject_dest; }
    }

 
    # Read Score and other hit info
    
    if( /^\s+Score.*Expect\s*=\s*(\S+)/ )
    {
        $expect = $1;
 
        $_ = <STDIN>;

        if( /Identities\s*=\s*(\S+)/ ) { $identities = $1; }
        else
        {
            warn "WARNING: [$query/$subject] Unable to parse 'Identities' in line: $_";
            $identities = 'error';
        }

        if( /Gaps\s*=\s*(\d+)\// ) { $gaps = $1; }
        else                       { $gaps = 0;  }
 
        $_ = <STDIN>;
        
        if( /\s+Strand =(.*)/ )
        {
	    $strand = $1;
	    $strand =~ s/\s//g;
	    if   ( $strand eq 'Plus/Plus'  ) { $sense =  1; }
	    elsif( $strand eq 'Plus/Minus' ) { $sense = -1; }
	    else {  print STDERR "WARNING 1 with senses: $strand\n"; }
	}
	else { print STDERR "WARNING: $query | $subject: Parse error for Strand with $_"; }
	
	finestruct_info_start()  if( $FINESTRUCT );
	
        # read positions
        
        $q_st = '';  $q_ed = '';  $s_st = '';  $s_ed = '';

        do {
            $_ = <STDIN>;        # query line of alignment
            
            if( /^Query:\s+(\d+)\s+(\S+)\s+(\d+)\s*$/ )
            {
	        $qst  = $1;
		$qseq = $2  if( $FINESTRUCT );
		$qed  = $3;
                if( $q_st eq ''  )           { $q_st = $qst; }
                if( $qst < $q_st )           { $q_st = $qst; }
                if( $qed < $q_st )           { $q_st = $qed; }
                if( !$q_ed or $qst > $q_ed ) { $q_ed = $qst; }
                if( $qed > $q_ed )           { $q_ed = $qed; }

		$_ = <STDIN>;        # the middle line - the bars
		
	    	if( $FINESTRUCT )
                {
		    $lng = length $qseq;
		    /^\s+(.{$lng})$/  or die "ERROR: $query | $subject: Parse error looking at bars with: $_";
	    	    $bars = $1;
	    	}
		
		$_ = <STDIN>;        # subject line of alignment
                
                /^Sbjct:\s+(\d+)\s+(\S+)\s+(\d+)\s*$/ or die;
	        $sst  = $1;
		$sseq = $2  if( $FINESTRUCT );
		$sed  = $3;
                if( !$s_st       )           { $s_st = $sst; }
                if( $sst < $s_st )           { $s_st = $sst; }
                if( $sed < $s_st )           { $s_st = $sed; }
                if( !$s_ed or $sst > $s_ed ) { $s_ed = $sst; }
                if( $sed > $s_ed )           { $s_ed = $sed; }
		
		finestruct_info()  if( $FINESTRUCT );
            }
        }
        while( (/^\s*$/ or /^[\s|]*$/ or /^Query:/ or /^Sbjct:/)  and not /^>/ );
        
        if( $BL2SEQ )
        {
            if( !$query     ) { $query     = 'xxxx'; }
            if( !$query_lng ) { $query_lng = '0000'; }
        }
        
        
	if( $FINESTRUCT )
        {
	    $finestruct = finestruct_info_end();
            if( $finestruct )
            {
	    	if( $FS_ENDS and $finestruct !~ /X/ )
                {
	    	    if   ( $q_ed <= $FS_ENDS              )  { $finestruct .= 'X';             }
	    	    elsif( $q_st >= $query_lng - $FS_ENDS )  { $finestruct  = 'X'.$finestruct; }
	    	    else  { die "ERROR: problem 2 with ($q_st,$q_ed) $finestruct\n"; }
	    	}
                $finestruct = " \t$finestruct";
            }
	}
        
        $out_str  = "$subject,$subject_lng,$s_st..$s_ed|";
        $out_str .= "$query,$query_lng,$q_st..$q_ed|";
        $out_str .= "$sense,$expect,$identities,$gaps";
        $out_str .= "$finestruct\n";
        
        $buf_str .= $out_str;  $buf_cnt++;     	# print $out_str;  but for buffering
        if( $buf_cnt >= $BUFFER )
        {
            print $buf_str;
            $out_byte_count += length $buf_str;
            $buf_str         = '';
            $buf_cnt         = 0;
        }
	
	if( $TEST_RUN ) { if( 10 == $TEST_RUN ) { last; } else { $TEST_RUN++; } }
        
        goto X;
    }
    
    if( $FINESTRUCT  and  /^Lambda/ )
    {
    	$_ = <STDIN>;
    	
        /^\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)/
            or  die "ERROR parsing parameters with: $_";
        #$lambda = $1;               # ?don't actually use this for anything
    	#$Kparam = $2;
    	#$Hparam = $3;               # ?don't actually use this for anything
    	
        #$param_str = "lambda = $lambda, K = $Kparam, H = $Hparam\n";
    	$param_str = "";
        if( $param_str ne $pre_param_str )
        {
            $buf_str      .= $param_str;
            $pre_param_str = $param_str;
    	}
    }
}

# finish up

if( $BUFFER ) { print $buf_str;  $out_byte_count += length $buf_str; }

close STDIN   if( $IN_FILE  );
close STDOUT  if( $OUT_FILE );

if( $OUT_FILE )            # check that it is the right size
{
    $file_size = -s $OUT_FILE;
    if( $file_size != $out_byte_count )
    {
        die  "ERROR: output file not right size\n"
	    ."    $OUT_FILE\n"
	    ."    should be $out_byte_count bytes but is $file_size\n";
    }
}

close STDERR  if( $ERR_FILE );


#===


sub finestruct_info_start
{
    $info          = '';
    $mismatchs     = '';
    $num_mismatchs = 0;
    $fsgaps        = 0;
    $lastgap       = 1;
    $qpos          = 0;
    $spos          = 0;
    $X1            = 0;
    $X2            = 0;
    
    return;
}


sub finestruct_info_end
{
    if( not $qpos    ) { return( '' ); }
    if( not $lastgap )
    {
        $info .= "$fsgaps"                                 if( $info =~ /\d[^X]+$/ );
        $info .= "|$stqb..$qpos:$stsb..$spos"."$mismatchs|";
    }
    return( $info );
}


sub finestruct_info
{
    # my() = @_;     get this sorted out..
    
    if( $FS_ENDS and ($qst  > ($FS_ENDS + 1) and $qed < ($query_lng - $FS_ENDS)) )  { return; }
    if( $FS_ENDS and ($q_st > ($FS_ENDS)     and $qed < ($query_lng - $FS_ENDS)) )  { return; }
    
    $qpos = $qst - 1;
    if( $sense == 1 ) { $spos = $sst - 1; }
    else	      { $spos = $sst + 1; }
    
    $qseq = reverse $qseq;
    $bars = reverse $bars;
    $sseq = reverse $sseq;
    
    while( $qch = chop $qseq )
    {
        $sch    = chop $sseq;
	$bch    = chop $bars;
	
	if( $FS_ENDS  and  $qpos > $FS_ENDS  and  $qpos < ($query_lng - $FS_ENDS) )
        {
	    &incriment;
	    next;
	}
	
	if( $qch eq '-'  or  $sch eq '-'  or  $qpos == $FS_ENDS )
        {
	    if( not $lastgap )
            {
		$info         .= $fsgaps   if( $stqb != $q_st and $info !~ /X$/ );
	        $info         .= "|$stqb..$qpos:$stsb..$spos"."$mismatchs|";
		$mismatchs     = '';
		$num_mismatchs = 0;
		$fsgaps        = 0;
	    }
	    $lastgap = 1;
	    $fsgaps++;
	}
        
	if( $FS_ENDS and $qpos == $FS_ENDS and $q_st <= $FS_ENDS and !$X1 ) { $info .= 'X'; $X1 = 1; &incriment; next; }
	if( $FS_ENDS and $qpos == ($query_lng - $FS_ENDS) and !$X2        ) { $info .= 'X'; $X2 = 1; }
	
	&incriment;
	
	if( $qch ne '-'  and  $sch ne '-' )
        {
	    if( $bch eq ' ' ) { $mismatchs .= ",$qpos"; $num_mismatchs++; }
	    if( $lastgap    ) { $stqb = $qpos;          $stsb = $spos;    }
	    $lastgap = 0;
	}
    }
}


sub incriment
{
    $qpos++  unless $qch eq '-';
    
    if( $sch ne '-' )
    {
       if( $sense == 1 ) { $spos++; }
       else		 { $spos--; }
    }
}


