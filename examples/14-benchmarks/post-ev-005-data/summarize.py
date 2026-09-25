import json, re, statistics
from pathlib import Path

root=Path(__file__).resolve().parent
rows=[]
source_hashes={}
for phase in ('completion','bench','confirm','profile'):
    for p in (root/phase).rglob('result.json'):
        result=json.loads(p.read_text());meta=json.loads((p.parent/'metadata.json').read_text())
        variant=result['variant']
        if variant in source_hashes:
            assert source_hashes[variant]==meta['server_hashes'],p
        source_hashes[variant]=meta['server_hashes']
        assert 'Future/PP.pm' in meta['loaded']['inc'] and 'Future/XS.pm' not in meta['loaded']['inc']
        assert '/pagi-ev-timer-' not in (meta['env'].get('PERL5LIB') or '')
        assert result['responses']==meta['requests']
        t=(p.parent/'process-time.txt').read_text()
        m=re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys',t)
        result.update(phase=phase, sample=str(p.relative_to(root)), measured_requests=meta['requests'],
            process_cpu_s=float(m[2])+float(m[3]),
            peak_rss_mib=int(re.search(r'(\d+)\s+maximum resident set size',t)[1])/1048576)
        rows.append(result)
summary={}
for phase in ('bench','confirm'):
    for case in sorted({x['case'] for x in rows if x['phase']==phase}):
        group={}
        for variant in ('release','main','experiment'):
            subset=[x for x in rows if x['phase']==phase and x['case']==case and x['variant']==variant]
            if subset:
                group[variant]={k:statistics.mean(x[k] for x in subset) for k in ('rps','process_cpu_s','peak_rss_mib')}
                group[variant]['rps_samples']=[x['rps'] for x in subset]
        for variant in ('main','experiment'):
            if variant in group:group[variant]['rps_delta_from_release_pct']=100*(group[variant]['rps']/group['release']['rps']-1)
        summary[phase+'/'+case]=group
(root/'measurements.json').write_text(json.dumps(dict(runs=rows,summary=summary),indent=2)+'\n')
print(json.dumps(summary,indent=2))

profiles={}
for p in (root/'profile').rglob('subs.json'):
    meta=json.loads((p.parent/'metadata.json').read_text())
    request_count=meta['requests']+meta['warmup_requests']+meta['preflight_requests']
    data=json.loads(p.read_text());byname={x['name']:x for x in data}
    assert byname['PAGI::Server::Connection::_handle_request']['calls']==request_count
    names=[
        'PAGI::Server::Connection::_create_receive','PAGI::Server::Connection::_create_scope',
        'PAGI::Server::Connection::_create_send','PAGI::Server::Connection::_h1_abort_hook',
        'PAGI::Server::Connection::_handle_request','PAGI::Server::Connection::_begin_body_discard',
        'PAGI::Server::Connection::_h1_end_scope_output','PAGI::Server::Connection::_wake_receive_pending',
        'PAGI::Server::ConnectionState::new','PAGI::Server::ConnectionState::_mark_complete',
        'PAGI::Server::ConnectionState::_deliver_terminal','PAGI::Server::EventValidator::validate_http_send',
        'PAGI::Server::EventValidator::scope_send_clean','Future::PP::new','Future::PP::on_done',
        'IO::Async::Loop::later','IO::Async::Loop::EV::watch_idle','EV::Timer::DESTROY',
    ]
    records=[]
    for name in names:
        x=byname.get(name,dict(name=name,calls=0,exclusive_s=0))
        records.append(dict(x,calls_per_request=x['calls']/request_count,
            exclusive_us_per_request=x['exclusive_s']*1e6/request_count))
    profiles[p.parent.name]=dict(request_count=request_count,selected=records)
(root/'profile-comparison.json').write_text(json.dumps(profiles,indent=2)+'\n')

import hashlib
for hashes in source_hashes.values():
    for path,digest in hashes.items():assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
print('Validated all samples and unchanged source hashes.')
