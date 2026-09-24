from pathlib import Path
import subprocess,shutil,re,hashlib,json
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
candidate=OUT/'candidate'
candidate.mkdir(exist_ok=True)
for d in ('lib','bin'):shutil.copytree(ROOT/d,candidate/d,dirs_exist_ok=True)
old=subprocess.check_output(['git','show','b8dd6ee:lib/PAGI/Server/Connection.pm'],cwd=ROOT,text=True)
f=candidate/'lib/PAGI/Server/Connection.pm';s=f.read_text();before=s
helper=old[old.index('sub _stream_write {'):old.index('# HTTP/1.1: the transport handle reads',old.index('sub _stream_write {'))]
start=s.index('sub _get_write_buffer_size {');end=s.index('# HTTP/1.1: the transport handle reads',start)
s=s[:start]+helper+s[end:]
start=s.index('sub start {');pos=s.index('    weaken(my $weak_self = $self);',start)+len('    weaken(my $weak_self = $self);')
s=s[:pos]+ '\n    $self->{_on_write_cb} = sub { $weak_self->{_outbound_bytes} -= $_[1] if $weak_self };' +s[pos:]
ctor='        _drain_waiters       => []'
assert ctor in s
s=s.replace(ctor,'        _outbound_bytes      => 0,\n'+ctor,1)
close="    $self->_cancel_drain_waiters('connection closing');"
assert s.count(close)==1
s=s.replace(close,close+'\n    $self->{_outbound_bytes} = 0;',1)
# Convert audited materialized-byte writes. Do not replace the helper's own
# stream->write call: it carries on_write and must stay the sole write site.
lines=s.splitlines(keepends=True);count=0
for i,line in enumerate(lines):
 if '$stream->write($out, on_write =>' in line:continue
 line,n=re.subn(r'\$(self|weak_self)->\{stream\}->write\(',r'$\1->_stream_write(',line);count+=n
 if '$stream->write(' in line:
  assert i>8500,(i,line)  # file/fh helpers only in the current source
  line=line.replace('$stream->write(','$self->_stream_write(');count+=1
 lines[i]=line
s=''.join(lines);assert s.count('->write(')==1,s.count('->write(')
f.write_text(s)
(OUT/'candidate.patch').write_text(subprocess.run(['diff','-u',str(ROOT/'lib/PAGI/Server/Connection.pm'),str(f)],capture_output=True,text=True).stdout)
identity={'base':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'source_commit':'b8dd6ee','converted_write_sites':count,'candidate_connection_sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'base_connection_sha256':hashlib.sha256(before.encode()).hexdigest(),'warning':'Diagnostic only. Known close/reset and rejected-write limitations retained; not a release candidate.'}
(OUT/'candidate.json').write_text(json.dumps(identity,indent=2)+'\n')
print(json.dumps(identity,indent=2))
