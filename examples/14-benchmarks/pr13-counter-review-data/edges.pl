use v5.36;
use IO::Async::Loop;
use IO::Async::Stream;
use PAGI::Server::Connection;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);
use Errno qw(EAGAIN EPIPE);
use Test::More;
use JSON::PP;
my @observations;
sub actual ($stream) {
    my $n=0;
    for my $w (@{$stream->{writequeue}}) {
        my $data=$w->data;
        $n+=length($data) if defined($data) && !ref($data);
    }
    return $n;
}
sub pair {
    socketpair(my $out,my $peer,AF_UNIX,SOCK_STREAM,PF_UNSPEC) or die $!;
    my $loop=IO::Async::Loop->new;
    my $mode='write';
    my $stream=IO::Async::Stream->new(write_handle=>$out, write_all=>0,
        writer=>sub {
            my ($s,$h,undef,$max)=@_;
            if ($mode eq 'blocked') { $!=EAGAIN;return undef }
            if ($mode eq 'error') { $!=EPIPE;return undef }
            my $n=length($_[2])<$max ? length($_[2]):$max;
            substr($_[2],0,$n)='';return $n;
        });
    $loop->add($stream);
    my $conn=PAGI::Server::Connection->new(stream=>$stream,timeout=>0,transport_type=>'unix');
    $conn->start;
    # Match current deferred writes; old start enables autoflush, so explicitly
    # reset it for edge observations of a pending queue.
    $stream->configure(autoflush=>0, write_len=>8192, write_all=>0);
    return ($conn,$stream,$peer,\$mode,$loop);
}
sub observe ($name,$conn,$stream) {
    push @observations, {case=>$name,raw=>$conn->{_outbound_bytes},reported=>$conn->_get_write_buffer_size,actual=>actual($stream)};
}
{
    my ($conn,$s,$peer,$mode,$loop)=pair();
    $conn->_stream_write('x' x 65536);
    is($conn->_get_write_buffer_size,actual($s),'enqueue counter matches queue');
    $$mode='blocked';$s->on_write_ready;
    is($conn->_get_write_buffer_size,65536,'EAGAIN does not subtract bytes');
    $$mode='write';$s->on_write_ready;
    is($conn->_get_write_buffer_size,actual($s),'partial write counter matches queue');
    is(actual($s),57344,'partial write consumed exactly 8 KiB');
    observe('partial-write',$conn,$s);
    $s->on_write_ready for 1..7;
    is($conn->_get_write_buffer_size,0,'full drain counter zero');
    $s->close_now;close $peer;
}
{
    my ($conn,$s,$peer,$mode,$loop)=pair();
    $conn->_stream_write('x' x 65536);
    $conn->_close;
    observe('after-close-before-drain',$conn,$s);
    is(actual($s),65536,'graceful close retains queued bytes');
    is($conn->_get_write_buffer_size,0,'old counter resets before graceful drain');
    $s->on_write_ready;
    observe('after-close-partial-drain',$conn,$s);
    is($conn->{_outbound_bytes},-8192,'later write callback makes raw counter negative');
    is(actual($s),57344,'stream still holds remaining bytes');
    $s->on_write_ready for 1..7;
    observe('after-close-full-drain',$conn,$s);
    is($conn->{_outbound_bytes},-65536,'raw counter ends negative after graceful drain');
    is($conn->_get_write_buffer_size,0,'getter clamp conceals negative raw counter');
    close $peer;
}
{
    my ($conn,$s,$peer,$mode,$loop)=pair();
    $conn->_stream_write('x' x 65536);
    $s->close_when_empty;
    my @warnings;
    {local $SIG{__WARN__}=sub { push @warnings,@_ };$conn->_stream_write('z' x 100)}
    observe('write-rejected-while-closing',$conn,$s);
    is(actual($s),65536,'closing stream rejects new bytes');
    is($conn->_get_write_buffer_size,65636,'old counter includes rejected bytes');
    like(join('',@warnings),qr/Stream that is closing/,'stream reports rejection');
    $s->close_now;close $peer;
}
{
    my ($conn,$s,$peer,$mode,$loop)=pair();
    $conn->_stream_write('x' x 65536);
    $$mode='error';$s->on_write_ready;
    observe('write-error-close',$conn,$s);
    ok($conn->{closed},'write error closes connection');
    is($conn->_get_write_buffer_size,0,'old counter reports zero after error');
    # Whether bytes remain depends on close_when_empty; record it as observation.
    $s->close_now;close $peer;
}
open my $fh,'>',$ARGV[0] or die $!;
print $fh JSON::PP->new->canonical->pretty->encode(\@observations);
close $fh;
done_testing;
