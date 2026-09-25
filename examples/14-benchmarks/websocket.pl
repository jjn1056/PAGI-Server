use strict;
use warnings;
use Future::AsyncAwait;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Expected WebSocket scope" unless $scope->{type} eq 'websocket';
    my $event = await $receive->();
    die "Expected websocket.connect" unless $event->{type} eq 'websocket.connect';
    await $send->({type => 'websocket.accept'});
    while (1) {
        my $event = await $receive->();
        last if $event->{type} eq 'websocket.disconnect';
        die "Expected WebSocket message" unless $event->{type} eq 'websocket.receive';
        my %payload = defined($event->{bytes})
            ? (bytes => $event->{bytes}) : (text => $event->{text});
        await $send->({type => 'websocket.send', %payload});
    }
};
$app;
