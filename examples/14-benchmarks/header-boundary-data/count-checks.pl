use strict;
use warnings;
use JSON::PP;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
{
    package Local::Stream;
    sub write { $_[0]{output} .= $_[1] }
    package Local::Connection;
    our @ISA = ('PAGI::Server::Connection');
    sub _get_write_buffer_size { 0 }
}
my %counts;
for my $name (qw(PAGI::Server::EventValidator::check_header_name PAGI::Server::EventValidator::check_header_value PAGI::Server::Protocol::HTTP1::_validate_header_name PAGI::Server::Protocol::HTTP1::_validate_header_value)) {
    no strict 'refs';
    no warnings 'redefine';
    my $original = \&{$name};
    *{$name} = sub { ++$counts{$name}; goto &$original };
}
my @rows;
for my $n (1, 10, 30) {
    my $conn = Local::Connection->new(stream => bless({}, 'Local::Stream'),
        protocol => PAGI::Server::Protocol::HTTP1->new);
    my $send = $conn->_create_send({method=>'GET',headers=>[]});
    my @headers = map { ['x-field-'.$_, 'some-value'] } 1..$n;
    %counts=();
    $send->({type=>'http.response.start',status=>200,headers=>\@headers,trailers=>1})->get;
    push @rows, {event=>'start',app_fields=>$n,checks=>{%counts}};
    $send->({type=>'http.response.body',body=>'ok',more=>0})->get;
    %counts=();
    $send->({type=>'http.response.trailers',headers=>\@headers})->get;
    push @rows, {event=>'trailers',app_fields=>$n,checks=>{%counts}};
}
print JSON::PP->new->canonical->pretty->encode(\@rows);
