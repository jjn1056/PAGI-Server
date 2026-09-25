use strict;
use warnings;
use Devel::NYTProf::Data;
use JSON::PP;
my $p = Devel::NYTProf::Data->new({filename => $ARGV[0], quiet => 1});
my @files;
for my $fi ($p->all_fileinfos) {
    next unless $fi->filename =~ m{PAGI/Server/(?:Connection|ConnectionState|TransportState|EventValidator|Protocol/HTTP1)\.pm$};
    my $data = $fi->line_time_data or next;
    my $source = $fi->srclines_array or die 'No source for '.$fi->filename;
    my $calls = $fi->sub_call_lines || {};
    my @rows;
    for my $line (1..$#$data) {
        next unless $data->[$line];
        my ($seconds,$count) = @{$data->[$line]};
        next unless $count || $seconds;
        push @rows, {line=>$line,seconds=>$seconds,count=>$count,
            text=>$source->[$line-1],calls=>$calls->{$line} || {}};
    }
    push @files, {file=>$fi->filename,lines=>\@rows};
}
print JSON::PP->new->canonical->pretty->encode(\@files);
