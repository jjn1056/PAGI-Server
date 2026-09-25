#!/bin/bash
set -exo pipefail
exec > >(tee /var/log/pagi-benchmark-bootstrap.log) 2>&1
cat >/etc/systemd/system/pagi-benchmark-autostop.service <<'EOF'
[Unit]
Description=Stop PAGI benchmark instance after eight hours
[Service]
Type=oneshot
ExecStart=/usr/sbin/shutdown -h now
EOF
cat >/etc/systemd/system/pagi-benchmark-autostop.timer <<'EOF'
[Unit]
Description=Eight hour PAGI benchmark cost backstop
[Timer]
OnBootSec=8h
Unit=pagi-benchmark-autostop.service
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now pagi-benchmark-autostop.timer
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential curl ca-certificates git perlbrew cpanminus python3 hey libssl-dev zlib1g-dev libnghttp2-dev sysstat
systemctl stop apt-daily.timer apt-daily-upgrade.timer
sudo -u ubuntu -H bash <<'USER'
set -exo pipefail
perlbrew init
source "$HOME/perl5/perlbrew/etc/bashrc"
perlbrew install --notest -j 4 perl-5.42.2
perlbrew switch perl-5.42.2
perlbrew install-cpanm
cpanm --notest IO::Async@0.805 Future@0.52 Future::AsyncAwait@0.71 HTTP::Parser::XS@0.17 Protocol::WebSocket@0.26 EV@4.37 IO::Async::Loop::EV@0.05 IO::Socket::IP URI Test2::V0 JSON::MaybeXS Net::Async::HTTP Net::Async::WebSocket::Client IO::Async::SSL
cpanm --notest PAGI
cpanm --reinstall https://cpan.metacpan.org/authors/id/J/JJ/JJNAPIORK/PAGI-Server-0.002013.tar.gz
mkdir -p "$HOME/pagi-benchmark"
touch "$HOME/pagi-benchmark/bootstrap-complete"
USER
