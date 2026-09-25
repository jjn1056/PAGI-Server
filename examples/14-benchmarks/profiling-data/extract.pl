use strict;
use warnings;
use Devel::NYTProf::Data;
use JSON::PP;
my $p = Devel::NYTProf::Data->new({filename => $ARGV[0], quiet => 1});
my @rows;
my $map = $p->subname_subinfo_map;
for my $name (sort keys %$map) {
    my $s=$map->{$name};
    next unless $s->calls;
    push @rows, {name=>$name, calls=>$s->calls, inclusive_s=>$s->incl_time,
        exclusive_s=>$s->excl_time, line=>$s->first_line,
        callers=>[sort keys %{$s->called_by_subnames}]};
}
print JSON::PP->new->canonical->pretty->encode(\@rows);
