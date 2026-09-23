from pathlib import Path
from statistics import mean
import json
P=Path(__file__).resolve().parent
names=['Future::PP::new','Future::PP::on_done','PAGI::Server::Connection::_create_receive','PAGI::Server::Connection::_create_send','PAGI::Server::Connection::_h1_abort_hook','PAGI::Server::ConnectionState::_mark_complete','PAGI::Server::ConnectionState::_deliver_terminal','IO::Async::Loop::later','IO::Async::Loop::EV::watch_idle','PAGI::Server::Connection::_h1_end_scope_output','PAGI::Server::Connection::_wake_receive_pending','PAGI::Server::Connection::_begin_body_discard','PAGI::Server::Connection::_discard_unread_body','PAGI::Server::EventValidator::scope_send_clean','PAGI::Server::Connection::_get_write_buffer_size','IO::Async::Stream::Writer::data']
out={}
for case,n in [('get',2203),('stream',703)]:
 variants={v:[] for v in ('release','experiment')}
 for i,v in enumerate(('release','experiment','experiment','release')):
  d=P/f'{case}-{i}'/f'{v}-{case}'
  rows={r['name']:r for r in json.loads((d/'subs.json').read_text())}
  assert rows['PAGI::Server::Connection::_handle_request']['calls']==n
  assert rows['Future::PP::new']['calls']==(8878 if case=='get' else 47167)
  meta=json.loads((d/'metadata.json').read_text())
  assert meta['loaded']['future_class']=='Future'
  assert meta['env']['PERL_FUTURE_NO_XS']=='1'
  assert meta['env']['PAGI_FUTURE_XS'] is None
  variants[v].append(rows)
 allnames=set(names)
 for v,rows in variants.items():
  for nm in rows[0]:
   if any(k in nm for k in ('Writer','6121]','6394]','4639]','ConnectionState.pm:748]')):allnames.add(nm)
 out[case]={nm:{v:{'calls':[r.get(nm,{}).get('calls',0) for r in rs],'mean_exclusive_s':mean(r.get(nm,{}).get('exclusive_s',0) for r in rs),'exclusive_us_per_request':mean(r.get(nm,{}).get('exclusive_s',0) for r in rs)*1e6/n} for v,rs in variants.items()} for nm in sorted(allnames)}
 print(case,'(us/request exclusive elapsed, profiled)')
 for nm in sorted(allnames):
  row=out[case][nm]
  print(nm.split('::')[-1], ' '.join(f"{v}: {d['calls']} {d['exclusive_us_per_request']:.3f}" for v,d in row.items()))
(P/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
