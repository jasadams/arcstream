"""Hand-counted full12 session oracle; no application transition import."""
from datetime import datetime,timezone

def ms(value):
    dt=datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    delta=dt-datetime(1970,1,1,tzinfo=timezone.utc)
    return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000

def fixture():
    inputs=[];timers=[];progress=None
    def event(tenant,session,canonical,event_id,at,event_type,**fields):
        nonlocal progress
        raw=ms(at);progress=max(progress,raw) if progress is not None else raw
        value=dict(kind='event',tenant_id=tenant,session_id=session,canonical_id=canonical,
                   event_id=event_id,generation=event_id,event_time=at,logical_time=at,
                   session_source_ms=raw,event_type=event_type,**fields)
        inputs.append(value)
    def clock(event_id,at):
        nonlocal progress
        raw=ms(at);progress=max(progress,raw)
        inputs.append(dict(kind='clock',tenant_id='clock',session_id='',canonical_id='clock',
                           generation=event_id,logical_time=at,event_time=at,session_source_ms=raw))
    def control(tenant,session,generation,at):
        inputs.append(dict(kind='session_summary_timeout',tenant_id=tenant,session_id=session,
                           generation=generation,logical_time=at))
    event('tenant-a','shared','first-a','a1','2020-01-31 23:59:00.000','page_view',
          page_url='/first',device_type='desktop',browser='Firefox',country='AU')
    event('tenant-a','shared','later-a','a2','2020-01-31 23:58:30.000','custom_event',
          page_url='/late',browser='Chrome',country='')
    event('tenant-b','shared','first-b','b1','2020-02-01 00:00:00.000','click',
          page_url='/tenant',device_type='tablet',browser='Safari',country='NZ')
    event('tenant-a','shared','ignored-canonical','a3','2020-02-01 00:01:00.000','custom_event',
          page_url='/first',device_type='mobile',browser='',country='US')
    clock('clock1','2020-02-01 00:32:00.000')
    control('tenant-a','shared','a3','2020-02-01 00:31:00.000')
    event('tenant-a','shared','new-a','a4','2020-02-01 00:40:00.000','login',
          page_url='/new',device_type='phone',browser='Safari',country='GB')
    # Prior global watermark already passed tenant-b's deadline while its
    # asynchronous timer feedback was delayed: emit old B before starting new B.
    event('tenant-b','shared','new-b','b2','2020-02-01 00:41:00.000','login',page_url='/newb')
    control('tenant-b','shared','b1','2020-02-01 00:30:00.000')
    event('tenant-a','shared','ignored-after-reopen','a5','2020-02-01 00:20:00.000','click',
          page_url='/oldlater',country='')
    clock('clock2','2020-02-01 01:12:00.000')
    control('tenant-a','shared','a5','2020-02-01 01:10:00.000')
    control('tenant-a','shared','a5','2020-02-01 01:10:00.000')
    control('tenant-b','shared','b2','2020-02-01 01:11:00.000')
    event('tenant-a',None,'ignored','empty','2020-02-01 01:20:00.000','page_view',page_url='/excluded')
    event('tenant-pre','pre','pre-canonical','pre','1969-12-31 23:59:59.000','custom',page_url='/before')
    control('tenant-pre','pre','pre','1970-01-01 00:30:00.000')
    expected=[
      dict(tenant_id='tenant-a',session_id='shared',canonical_id='first-a',start_time='2020-01-31 23:59:00.000',
           end_time='2020-02-01 00:01:00.000',duration_sec=120,event_count=3,pages=['/first','/late'],
           event_types={'custom_event':2,'page_view':1},device_type='mobile',browser='Chrome',country='US'),
      dict(tenant_id='tenant-b',session_id='shared',canonical_id='first-b',start_time='2020-02-01 00:00:00.000',
           end_time='2020-02-01 00:00:00.000',duration_sec=0,event_count=1,pages=['/tenant'],
           event_types={'click':1},device_type='tablet',browser='Safari',country='NZ'),
      dict(tenant_id='tenant-a',session_id='shared',canonical_id='new-a',start_time='2020-02-01 00:40:00.000',
           end_time='2020-02-01 00:40:00.000',duration_sec=0,event_count=2,pages=['/new','/oldlater'],
           event_types={'click':1,'login':1},device_type='phone',browser='Safari',country='GB'),
      dict(tenant_id='tenant-b',session_id='shared',canonical_id='new-b',start_time='2020-02-01 00:41:00.000',
           end_time='2020-02-01 00:41:00.000',duration_sec=0,event_count=1,pages=['/newb'],
           event_types={'login':1},device_type='',browser='',country=''),
      dict(tenant_id='tenant-pre',session_id='pre',canonical_id='pre-canonical',start_time='1969-12-31 23:59:59.000',
           end_time='1970-01-01 00:00:00.000',duration_sec=1,event_count=1,pages=['/before'],
           event_types={'custom':1},device_type='',browser='',country='')]
    assert all(len(row)==12 for row in expected)
    # Hand-listed timer dispatches. Each input event/clock advances stream
    # progress, while session controls never advance the source watermark.
    for tenant,session,generation,kind,deadline,watermark in [
      ('tenant-a','shared','a1','session_summary_timeout','2020-02-01 00:29:00.000','2020-01-31 23:58:54.999'),
      ('tenant-a','shared','a2','session_summary_timeout','2020-02-01 00:29:00.000','2020-01-31 23:58:54.999'),
      ('tenant-b','shared','b1','session_summary_timeout','2020-02-01 00:30:00.000','2020-01-31 23:59:54.999'),
      ('tenant-a','shared','a3','session_summary_timeout','2020-02-01 00:31:00.000','2020-02-01 00:00:54.999'),
      ('clock','','clock1','clock','2020-02-01 00:32:00.000','2020-02-01 00:31:54.999'),
      ('tenant-a','shared','a4','session_summary_timeout','2020-02-01 01:10:00.000','2020-02-01 00:39:54.999'),
      ('tenant-b','shared','b2','session_summary_timeout','2020-02-01 01:11:00.000','2020-02-01 00:40:54.999'),
      ('tenant-a','shared','a5','session_summary_timeout','2020-02-01 01:10:00.000','2020-02-01 00:40:54.999'),
      ('clock','','clock2','clock','2020-02-01 01:12:00.000','2020-02-01 01:11:54.999'),
      ('tenant-a','','empty','clock','2020-02-01 01:20:00.000','2020-02-01 01:19:54.999'),
      ('tenant-pre','pre','pre','session_summary_timeout','1970-01-01 00:30:00.000','2020-02-01 01:19:54.999')]:
        timers.append(dict(tenant_id=tenant,session_id=session,generation=generation,kind=kind,
                           deadline_time=deadline,progress_time=watermark))
    return dict(inputs=inputs,expected=expected,timer_seeds=timers,checkpoint_after=6)
