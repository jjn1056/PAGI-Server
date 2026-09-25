from pathlib import Path
import re
p=Path('lib/PAGI/Server/Connection.pm');s=p.read_text()
a=s.index('sub _create_send {');b=s.index('\n# Flush any response headers',a)
part=s[a:b]
prefix,body=part.split('    return async sub {\n',1)
assert body.endswith('    };\n}\n')
body=body[:-len('    };\n}\n')]
body=body.replace('        my ($event) = @_;\n','',1)
body='\n'.join(line[4:] if line.startswith('    ') else line for line in body.split('\n'))
for name in ['chunked','expects_trailers','seq','publish','is_refusal','is_head_request','http_version','is_http10','client_wants_keepalive']:
 body=re.sub(r'\$'+name+r'\b',lambda m:'$state->{'+name+'}',body)
prefix=prefix.replace("    my $chunked = 0;\n    my $expects_trailers = 0;\n    my $seq = 'initial';\n",'')
prefix=prefix.replace("    $publish->($seq);", "    $publish->('initial');")
prefix=prefix.replace("    # Publish the closure-local $seq where the scope's owner reads it, so the", "    # Publish the private send state where the scope's owner reads it, so the")
replacement=prefix+'''    my $state = {
        chunked => 0, expects_trailers => 0, seq => 'initial',
        publish => $publish, is_refusal => $is_refusal,
        is_head_request => $is_head_request, http_version => $http_version,
        is_http10 => $is_http10, client_wants_keepalive => $client_wants_keepalive,
    };
    return sub {
        return Future->done unless $weak_self;
        return $weak_self->_send_http_event($state, @_);
    };
}

# One coroutine body shared by all HTTP sends. Shift before weakening so @_
# cannot retain the connection while a write or file read is suspended.
async sub _send_http_event {
    weaken(my $weak_self = shift);
    my ($state, $event) = @_;
'''+body+'}\n'
p.write_text(s[:a]+replacement+s[b:])
