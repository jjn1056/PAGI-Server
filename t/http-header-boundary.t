use strict;
use warnings;
use Test2::V0;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;

# Exercise the real send boundary and serialized bytes. The sink only replaces
# socket I/O; neither validation, sequencing nor formatting is mocked.
{
    package Local::HeaderSink;
    sub write { $_[0]{bytes} .= $_[1]; }
    package Local::HeaderConnection;
    our @ISA = ('PAGI::Server::Connection');
    sub _get_write_buffer_size { 0 }
}
sub setup {
    my ($method) = @_;
    my $sink = bless { bytes => '' }, 'Local::HeaderSink';
    my $conn = Local::HeaderConnection->new(stream => $sink,
        protocol => PAGI::Server::Protocol::HTTP1->new);
    $conn->{current_connection_state} = PAGI::Server::ConnectionState->new(connection => $conn);
    my $send = $conn->_create_send({ method => $method // 'GET', headers => [] });
    return ($conn, $sink, $send);
}

subtest 'bad start cannot consume the initial state, even for a stripped field' => sub {
    my ($conn, $sink, $send) = setup();
    my $bad = $send->({type=>'http.response.start', status=>200,
        headers=>[['connection', "close\r\nInjected: yes"]]});
    ok($bad->is_failed, 'invalid start fails');
    like(($bad->failure)[0], qr/Invalid header value/, 'byte-safety error');
    is($sink->{bytes}, '', 'no bytes emitted');
    ok(!$conn->{current_connection_state}->response_started, 'response has not started');
    my $headers = [['set-cookie','a=1'],['set-cookie','b=2'],['x-bytes',"tab\tand\xff"]];
    my $copy = [map { [@$_] } @$headers];
    my $start = $send->({type=>'http.response.start',status=>200,headers=>$headers});
    ok($start->is_done, 'valid start can retry');
    $start->get;
    $send->({type=>'http.response.body',body=>'ok',more=>0})->get;
    like($sink->{bytes}, qr/set-cookie: a=1\r\nset-cookie: b=2\r\nx-bytes: tab\tand\xff\r\n/, 'duplicates, order and permitted bytes survive');
    is($headers,$copy,'application headers unchanged');
    ok($conn->{current_connection_state}->response_complete, 'retried response completes');
};

for my $method ('GET', 'HEAD') {
    subtest "$method rejects invalid trailers and allows a valid retry" => sub {
        my ($conn,$sink,$send) = setup($method);
        $send->({type=>'http.response.start',status=>200,headers=>[],trailers=>1})->get;
        $send->({type=>'http.response.body',body=>'ok',more=>0})->get;
        my $before = $sink->{bytes};
        my $bad = $send->({type=>'http.response.trailers',headers=>[['connection',"bad\0value"]]});
        ok($bad->is_failed,'bad trailer fails even when stripped or HEAD-discarded');
        like(($bad->failure)[0],qr/Invalid header value/,'byte-safety error');
        is($sink->{bytes},$before,'failed trailers emit nothing');
        ok(!$conn->{current_connection_state}->response_complete,'bad trailers do not complete the response');
        my $trailers = [['x-trailer','one'],['x-trailer','two']];
        my $good = $send->({type=>'http.response.trailers',headers=>$trailers});
        ok($good->is_done,'valid trailers can retry');
        $good->get;
        ok($conn->{current_connection_state}->response_complete,'valid retry completes');
        if ($method eq 'GET') {
            like($sink->{bytes},qr/0\r\nx-trailer: one\r\nx-trailer: two\r\n\r\n\z/,'duplicate trailers remain ordered');
        } else {
            unlike($sink->{bytes},qr/x-trailer:/,'HEAD suppresses trailers');
        }
        is($trailers,[['x-trailer','one'],['x-trailer','two']],'application trailers unchanged');
    };
}

done_testing;
