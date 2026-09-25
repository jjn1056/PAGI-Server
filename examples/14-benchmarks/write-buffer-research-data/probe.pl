use v5.36;
use IO::Async::Loop;
use IO::Async::Stream;
use PAGI::Server::Connection;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);
use Errno qw(EAGAIN);
use JSON::PP;
use Test::More;

my @results;
for my $count (1, 8, 64, 256) {
    for my $mode (qw(plain on_write on_flush writer_counter)) {
        socketpair(my $out, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die $!;
        my $loop = IO::Async::Loop->new;
        my ($attempts, $writes, $written, $callbacks, $queued, $empty, $visits, $first_scan, $first_counter) = (0) x 9;
        my $bytes = $count * 1024;
        my $conn;
        my $stream = IO::Async::Stream->new(
            write_handle => $out,
            write_all => 1,
            writer => sub {
                my ($s, $handle, undef, $max) = @_;
                # Exercise one would-block result before successful partial writes.
                if (!$attempts++) { $! = EAGAIN; return undef; }
                my $n = length($_[2]) < $max ? length($_[2]) : $max;
                substr($_[2], 0, $n) = '';
                ++$writes;
                $written += $n;
                $queued -= $n if $mode eq 'writer_counter';
                # At this point the stream has consumed bytes but hasn't yet
                # invoked on_write/on_flush. Observe from that public callback
                # below for a fair on_write comparison.
                return $n;
            },
            on_outgoing_empty => sub { $empty = 1 },
        );
        $conn = bless { stream => $stream }, 'PAGI::Server::Connection';
        $loop->add($stream);
        for (1..$count) {
            $visits += scalar @{$stream->{writequeue}}; # Diagnostic only.
            is($conn->_get_write_buffer_size, ($_-1)*1024, "$mode/$count pre-write measurement") if $_ == $count;
            $queued += 1024;
            my %opt;
            if ($mode eq 'on_write') {
                $opt{on_write} = sub {
                    my ($s, $n) = @_;
                    $queued -= $n; ++$callbacks;
                    is($queued, $conn->_get_write_buffer_size, "on_write accounts for partial flush") if $callbacks == 1;
                };
            } elsif ($mode eq 'on_flush') {
                $opt{on_flush} = sub { $queued -= 1024; ++$callbacks; };
            }
            $stream->write('x' x 1024, %opt);
            $visits += scalar @{$stream->{writequeue}};
            is($conn->_get_write_buffer_size, $_*1024, "$mode/$count post-write measurement") if $_ == $count;
        }
        # Drive one readiness event: first attempt returns EAGAIN, no progress.
        $stream->on_write_ready;
        is($conn->_get_write_buffer_size, $bytes, "$mode/$count EAGAIN keeps all bytes");
        $stream->on_write_ready;
        is($written, $bytes, "$mode/$count writes all bytes");
        is($conn->_get_write_buffer_size, 0, "$mode/$count empty queue");
        is($queued, 0, "$mode/$count counter returns to zero") unless $mode eq 'plain';
        is($visits, $count*$count, "$mode/$count quadratic queue entries visited");
        push @results, { chunks=>$count, mode=>$mode, queue_entries_inspected=>$visits,
            successful_writer_calls=>$writes, callbacks=>$callbacks, bytes=>$written,
            would_block_attempts=>$attempts-$writes };
        $stream->close_now;
        close $peer;
    }
}

# A flush callback cannot report partial progress for a larger individual write.
{
    socketpair(my $out, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die $!;
    my $loop=IO::Async::Loop->new;
    my ($queued,$flushed)=(65536,0);
    my $stream=IO::Async::Stream->new(write_handle=>$out,write_all=>0,
        writer=>sub { my ($s,$h,undef,$n)=@_; substr($_[2],0,$n)=''; return $n });
    $loop->add($stream);
    $stream->write('x' x 65536,on_flush=>sub { $queued=0; $flushed++ });
    $stream->on_write_ready;
    my $conn=bless {stream=>$stream},'PAGI::Server::Connection';
    my $actual=$conn->_get_write_buffer_size;
    is($actual,57344,'partial write leaves 56 KiB buffered');
    is($queued,65536,'on_flush counter still reports 64 KiB');
    is($flushed,0,'on_flush has not fired during partial write');
    push @results,{case=>'partial_flush',actual_bytes=>$actual,on_flush_counter=>$queued};
    $stream->close_now;close $peer;
}
open my $fh,'>', $ARGV[0] or die $!;
print $fh JSON::PP->new->canonical->pretty->encode(\@results);
close $fh;
done_testing;
